const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');
const multer = require('multer');
const { Readable } = require('stream');
const { S3Client, DeleteObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');

process.on('uncaughtException', (err) => {
    console.error(err);
});
process.on('unhandledRejection', (reason) => {
    console.error(reason);
});

const PORT = process.env.PORT || 3000;
const UPLOAD_JSON = path.join(__dirname, 'upload.json');

const MAX_FILE_SIZE = 7 * 1024 * 1024 * 1024;
const MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;

const R2_BASE_URL = 'https://pub-e2d76735e9dd42f2af664d9e64599ca6.r2.dev';
const ICON_URL = 'https://adamdh7.org/adamdh7.png';
const R2_BUCKET = 'bref';

const s3 = new S3Client({
    region: 'auto',
    endpoint: 'https://49bdcdc6f29c08eda8bb7bcb8db9e27f.r2.cloudflarestorage.com',
    maxAttempts: 5,
    credentials: {
        accessKeyId: 'f0f6afdccc64b458f4d86110918e11ce',
        secretAccessKey: 'de5455c6af1e858d598d94d0de10717493133998d8e9cff54110311f744b266c'
    }
});

let mappings = {};

function loadMappingsFromDisk() {
    try {
        if (fs.existsSync(UPLOAD_JSON)) {
            mappings = JSON.parse(fs.readFileSync(UPLOAD_JSON, 'utf8') || '{}');
        } else {
            mappings = {};
        }
    } catch (err) {
        mappings = {};
    }
}

function saveMappingsToDisk() {
    try {
        const tmp = UPLOAD_JSON + '.tmp';
        fs.writeFileSync(tmp, JSON.stringify(mappings, null, 2));
        fs.renameSync(tmp, UPLOAD_JSON);
    } catch (err) {}
}

loadMappingsFromDisk();

function genToken() {
    const chars = '0123456789';
    let t = '';
    for (let i = 0; i < 7; i++) t += chars[Math.floor(Math.random() * chars.length)];
    return t;
}

function safeFileName(name) {
    const ext = path.extname(name || '');
    const base = path.basename(name || '', ext);
    const safeBase = base.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 120);
    const safeExt = ext.replace(/[^a-zA-Z0-9.]/g, '');
    return (safeBase + safeExt) || 'file';
}

function contentTypeFromName(filename) {
    const ext = path.extname(filename).toLowerCase();
    const map = {
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.gif': 'image/gif',
        '.webp': 'image/webp',
        '.bmp': 'image/bmp',
        '.svg': 'image/svg+xml',
        '.ico': 'image/x-icon',
        '.tiff': 'image/tiff',
        '.heic': 'image/heic',
        '.heif': 'image/heif',
        '.avif': 'image/avif',
        '.mp4': 'video/mp4',
        '.webm': 'video/webm',
        '.mov': 'video/quicktime',
        '.quicktime': 'video/quicktime',
        '.mkv': 'video/x-matroska',
        '.avi': 'video/x-msvideo',
        '.wmv': 'video/x-ms-wmv',
        '.flv': 'video/x-flv',
        '.m4v': 'video/x-m4v',
        '.3gp': 'video/3gpp',
        '.ts': 'video/mp2t',
        '.ogv': 'video/ogg',
        '.mpeg': 'video/mpeg',
        '.mpg': 'video/mpeg',
        '.m2ts': 'video/mp2t',
        '.mp3': 'audio/mpeg',
        '.wav': 'audio/wav',
        '.ogg': 'audio/ogg',
        '.m4a': 'audio/mp4',
        '.flac': 'audio/flac',
        '.aac': 'audio/aac',
        '.opus': 'audio/opus',
        '.mid': 'audio/midi',
        '.midi': 'audio/midi',
        '.pdf': 'application/pdf',
        '.doc': 'application/msword',
        '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        '.xls': 'application/vnd.ms-excel',
        '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.ppt': 'application/vnd.ms-powerpoint',
        '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        '.odt': 'application/vnd.oasis.opendocument.text',
        '.rtf': 'application/rtf',
        '.txt': 'text/plain',
        '.html': 'text/html',
        '.htm': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.mjs': 'application/javascript',
        '.json': 'application/json',
        '.xml': 'application/xml',
        '.csv': 'text/csv',
        '.md': 'text/markdown',
        '.yaml': 'text/yaml',
        '.yml': 'text/yaml',
        '.zip': 'application/zip',
        '.rar': 'application/vnd.rar',
        '.7z': 'application/x-7z-compressed',
        '.tar': 'application/x-tar',
        '.gz': 'application/gzip',
        '.woff': 'font/woff',
        '.woff2': 'font/woff2',
        '.ttf': 'font/ttf',
        '.otf': 'font/otf',
        '.epub': 'application/epub+zip',
        '.apk': 'application/vnd.android.package-archive',
        '.exe': 'application/vnd.microsoft.portable-executable',
        '.bin': 'application/octet-stream'
    };
    return map[ext] || 'application/octet-stream';
}

function isImageFile(filename) {
    return ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.svg', '.avif', '.heic', '.heif', '.tiff', '.ico'].includes(path.extname(filename).toLowerCase());
}

function isVideoFile(filename) {
    return ['.mp4', '.webm', '.m4v', '.mov', '.ogg', '.ogv', '.quicktime', '.mkv', '.avi', '.wmv', '.flv', '.3gp', '.ts', '.mpeg', '.mpg', '.m2ts'].includes(path.extname(filename).toLowerCase());
}

function isPreviewableFile(filename, mime) {
    return isImageFile(filename) || isVideoFile(filename);
}

function escapeHtml(text) {
    return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function wantsHtmlPreview(req) {
    const accept = String(req.headers.accept || '').toLowerCase();
    const dest = String(req.headers['sec-fetch-dest'] || '').toLowerCase();
    if (dest === 'video' || dest === 'audio' || dest === 'image' || dest === 'empty') return false;
    if (dest === 'document' || dest === 'iframe' || dest === 'object') return true;
    if (accept.includes('text/html')) return true;
    return false;
}

function buildViewerHtml(title, mediaUrl, filename) {
    const safeTitle = escapeHtml(title);
    const safeMediaUrl = escapeHtml(mediaUrl);
    return `<!doctype html>
<html lang="ht">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${safeTitle}</title>
<link rel="icon" type="image/png" href="${ICON_URL}">
<link rel="shortcut icon" type="image/png" href="${ICON_URL}">
<link rel="apple-touch-icon" href="${ICON_URL}">
<meta name="theme-color" content="#000000">
<style>
html, body { margin: 0; padding: 0; width: 100vw; height: 100vh; height: 100dvh; background-color: #000000; display: flex; align-items: center; justify-content: center; overflow: hidden; }
img { max-width: 100%; max-height: 100%; object-fit: contain; outline: none; border: none; background-color: transparent; }
</style>
</head>
<body><img src="${safeMediaUrl}" alt="${safeTitle}"></body></html>`;
}

function buildCustomPlayerHtml(title, targetUrl, fullUrl, mimeType) {
    const pageTitle = escapeHtml(title);
    const safeTargetUrl = escapeHtml(targetUrl);
    const safeFullUrl = escapeHtml(fullUrl);
    const safeMime = escapeHtml(mimeType || 'video/mp4');

    return `<!doctype html>
<html lang="ht">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"/>
<title>${pageTitle}</title>
<meta property="og:type" content="video.other" />
<meta property="og:title" content="${pageTitle}" />
<meta property="og:description" content="Gade ${pageTitle} sou Adam_D’H7" />
<meta property="og:video" content="${safeTargetUrl}" />
<meta property="og:video:secure_url" content="${safeTargetUrl}" />
<meta property="og:video:type" content="${safeMime}" />
<meta property="og:video:width" content="1280" />
<meta property="og:video:height" content="720" />
<meta property="og:image" content="${ICON_URL}" />
<meta name="twitter:card" content="player" />
<meta name="twitter:site" content="@adam_dh7" />
<meta name="twitter:title" content="${pageTitle}" />
<meta name="twitter:description" content="Gade ${pageTitle} sou Adam_D’H7" />
<meta name="twitter:player" content="${safeFullUrl}" />
<meta name="twitter:player:width" content="1280" />
<meta name="twitter:player:height" content="720" />
<meta name="twitter:player:stream" content="${safeTargetUrl}" />
<meta name="twitter:player:stream:content_type" content="${safeMime}" />
<meta name="twitter:image" content="${ICON_URL}" />
<link rel="manifest" href="manifest.json" />
<meta name="theme-color" content="#000000" />
<meta name="mobile-web-app-capable" content="yes" />
<meta name="apple-mobile-web-app-capable" content="yes" />
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent" />
<meta name="apple-mobile-web-app-title" content="${pageTitle}" />
<link rel="apple-touch-icon" href="${ICON_URL}" />
<link rel="preload" href="${safeTargetUrl}" as="video" />
<style>
  :root{
    --bg:#000;
    --muted:#9aa0a6;
    --accent:#fff;
    --seek-height:8px;
  }
  *{box-sizing:border-box}
  html,body{width:100%;height:100%;margin:0;padding:0;background:var(--bg);color:var(--accent);font-family:Inter,system-ui,Arial,sans-serif;overflow:hidden;}
  .wrap, .video-card, .controls-wrap, .inside-mini-controls { user-select:none; -webkit-user-select:none; -ms-user-select:none; -moz-user-select:none; }
  .wrap{position:absolute;inset:0;width:100%;height:100%;margin:0;padding:0;}
  .video-card{position:absolute;inset:0;border-radius:0;overflow:hidden;background:#000;width:100%;height:100%;touch-action:none}
  video{position:absolute;inset:0;width:100%;height:100%;object-fit:contain;z-index:1;background:#000;transition:filter .08s linear}
  .controls-wrap{position:absolute;left:12px;right:12px;bottom:12px;pointer-events:none;z-index:12;transition:opacity .25s ease;filter:drop-shadow(0px 2px 8px rgba(0,0,0,0.8));}
  .time-row{display:flex;align-items:center;gap:12px;padding:8px 12px;color:var(--muted);font-size:14px;justify-content:center}
  .time{width:82px;text-align:center;font-variant-numeric:tabular-nums;text-shadow:0px 2px 4px rgba(0,0,0,0.9);}
  .progress{flex:1;max-width:820px;display:flex;align-items:center}
  .seek{position:relative;height:var(--seek-height);width:100%;border-radius:999px;cursor:pointer;background:rgba(255,255,255,0.22);}
  .fill{position:absolute;left:0;top:0;height:100%;width:0%;border-radius:999px;background:linear-gradient(90deg, rgba(255,255,255,0.95), rgba(255,255,255,0.55));}
  .thumb{position:absolute;top:50%;transform:translate(-50%,-50%);width:18px;height:18px;border-radius:50%;background:#ffffff;box-shadow:0px 1px 4px rgba(0,0,0,0.8);pointer-events:auto;touch-action:none}
  .controls-hidden{opacity:0;pointer-events:none}
  .controls-visible{opacity:1;pointer-events:auto}
  .inside-mini-controls{
    position:absolute;
    left:12px;
    right:12px;
    top:50%;
    transform:translateY(-50%);
    z-index:13;
    display:flex;
    justify-content:center;
    gap:45px;
    pointer-events:auto;
    transition:opacity .25s ease;
  }
  .inside-hidden{opacity:0;pointer-events:none}
  .inside-visible{opacity:1;pointer-events:auto}
  .inside-item{
    display:flex;
    align-items:center;
    justify-content:center;
    background:none;
    border:none;
  }
  .inside-item .mini-btn{
    width:55px;
    height:55px;
    border-radius:50%;
    border:none;
    background:transparent;
    color:var(--accent);
    font-weight:700;
    font-size:26px;
    display:inline-flex;
    align-items:center;
    justify-content:center;
    cursor:pointer;
    -webkit-tap-highlight-color: transparent;
    filter:drop-shadow(0px 3px 6px rgba(0,0,0,0.9));
  }
  .inside-item .mini-btn:active{transform:scale(0.85)}
  .landscape-btn{margin-left:8px;border:none;background:transparent;color:var(--muted);width:30px;height:30px;cursor:pointer;filter:drop-shadow(0px 2px 4px rgba(0,0,0,0.9));display:flex;align-items:center;justify-content:center;}
  .landscape-btn:hover{color:var(--accent)}
  .swipe-indicator-left, .swipe-indicator-right {
    position:absolute;
    top:50%;
    transform:translateY(-50%);
    color:var(--accent);
    display:flex;
    flex-direction:column;
    align-items:center;
    gap:8px;
    z-index:20;
    pointer-events:none;
    opacity:0;
    transition:opacity .15s ease;
    font-weight:700;
    font-size:16px;
    text-shadow:0px 2px 8px rgba(0,0,0,0.9);
  }
  .swipe-indicator-left { left: 40px; }
  .swipe-indicator-right { right: 40px; }
  .swipe-indicator-left.show, .swipe-indicator-right.show { opacity:1; }
  .swipe-icon{width:26px;height:26px;}
  .spinner {
    animation: rotate 2s linear infinite;
    width: 50px;
    height: 50px;
    filter: drop-shadow(0px 2px 6px rgba(0,0,0,0.8));
  }
  .spinner .path {
    stroke: var(--accent);
    stroke-linecap: round;
    animation: dash 1.5s ease-in-out infinite;
  }
  @keyframes rotate {
    100% { transform: rotate(360deg); }
  }
  @keyframes dash {
    0% { stroke-dasharray: 1, 150; stroke-dashoffset: 0; }
    50% { stroke-dasharray: 90, 150; stroke-dashoffset: -35; }
    100% { stroke-dasharray: 90, 150; stroke-dashoffset: -124; }
  }
  #errorOverlay {
    display: none;
    position: absolute;
    inset: 0;
    background: #000;
    z-index: 50;
    justify-content: center;
    align-items: center;
    flex-direction: column;
  }
  .error-container {
    position: relative;
    width: 100%;
    height: 100%;
    display: flex;
    justify-content: center;
    align-items: center;
  }
  .error-image-wrapper {
    position: relative;
    width: 100%;
    max-width: 100%;
    aspect-ratio: 16/9;
    display: flex;
    justify-content: center;
    align-items: center;
  }
  .error-image-wrapper img {
    width: 100%;
    height: 100%;
    object-fit: contain;
  }
  #errTxt {
    position: absolute;
    bottom: 5%;
    width: 100%;
    text-align: center;
    color: grey;
    font-size: clamp(12px, 2vw, 16px);
    z-index: 10000;
    font-weight: bold;
    text-shadow: 0 1px 2px #000;
  }
</style>
</head>
<body>
  <div class="wrap">
    <div class="video-card" id="card">
      <video id="video" preload="auto" playsinline crossorigin="anonymous">
        <source src="${safeTargetUrl}" type="${safeMime}">
      </video>
      <div id="errorOverlay">
        <div class="error-container">
          <div class="error-image-wrapper">
            <img src="https://adamdh7.org/asset/nwa.png" alt="Erè" />
            <div id="errTxt"></div>
          </div>
        </div>
      </div>
      <div class="inside-mini-controls inside-visible" id="insideMini">
        <div class="inside-item" id="backContainer" style="display:none;"><button class="mini-btn" id="insideBack">-10</button></div>
        <div class="inside-item" id="playContainer" style="display:none;"><button class="mini-btn" id="insidePlay">❚❚</button></div>
        <div class="inside-item" id="spinnerContainer" style="display:flex;">
          <svg class="spinner" viewBox="0 0 50 50">
            <circle class="path" cx="25" cy="25" r="20" fill="none" stroke-width="5"></circle>
          </svg>
        </div>
        <div class="inside-item" id="forwardContainer" style="display:none;"><button class="mini-btn" id="insideForward">+10</button></div>
      </div>
      <div class="controls-wrap controls-visible" id="controlsWrap">
        <div class="time-row" id="timeRow">
          <div class="time" id="current">0:00</div>
          <div class="progress">
            <div class="seek" id="seekBar" tabindex="0" role="slider" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0">
              <div class="fill" id="fill"></div>
              <div class="thumb" id="thumb" aria-hidden="true"></div>
            </div>
          </div>
          <div class="time" id="duration">0:00</div>
          <button class="landscape-btn" id="landscapeBtn"></button>
        </div>
      </div>
      <div class="swipe-indicator-left" id="swipeIndicatorLeft" aria-hidden="true">
        <div class="swipe-icon" id="swipeIconLeft"></div>
        <div class="swipe-value" id="swipeValueLeft">0%</div>
      </div>
      <div class="swipe-indicator-right" id="swipeIndicatorRight" aria-hidden="true">
        <div class="swipe-icon" id="swipeIconRight"></div>
        <div class="swipe-value" id="swipeValueRight">0%</div>
      </div>
    </div>
  </div>
<script>
(function(){
  const card = document.getElementById('card');
  const video = document.getElementById('video');
  const insidePlay = document.getElementById('insidePlay');
  const insideForward = document.getElementById('insideForward');
  const insideBack = document.getElementById('insideBack');
  const backContainer = document.getElementById('backContainer');
  const playContainer = document.getElementById('playContainer');
  const forwardContainer = document.getElementById('forwardContainer');
  const spinnerContainer = document.getElementById('spinnerContainer');
  const seekBar = document.getElementById('seekBar');
  const fill = document.getElementById('fill');
  const thumb = document.getElementById('thumb');
  const currentEl = document.getElementById('current');
  const durationEl = document.getElementById('duration');
  const landscapeBtn = document.getElementById('landscapeBtn');
  const controlsWrap = document.getElementById('controlsWrap');
  const insideMini = document.getElementById('insideMini');
  const swipeIndicatorLeft = document.getElementById('swipeIndicatorLeft');
  const swipeIconLeft = document.getElementById('swipeIconLeft');
  const swipeValueLeft = document.getElementById('swipeValueLeft');
  const swipeIndicatorRight = document.getElementById('swipeIndicatorRight');
  const swipeIconRight = document.getElementById('swipeIconRight');
  const swipeValueRight = document.getElementById('swipeValueRight');
  const errorOverlay = document.getElementById('errorOverlay');
  const errTxt = document.getElementById('errTxt');

  const svgBrightness = '<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" width="26" height="26"><path d="M12 4V2M12 22v-2M4.93 4.93L3.51 3.51M20.49 20.49l-1.42-1.42M4 12H2M22 12h-2M4.93 19.07l-1.42 1.42M20.49 3.51l-1.42 1.42M12 8a4 4 0 100 8 4 4 0 000-8z" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  const svgVolume = '<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" width="26" height="26"><path d="M11 5L6 9H2v6h4l5 4V5z" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M19 9a5 5 0 010 6" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  const svgFullscreenEnter = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"></path></svg>';
  const svgFullscreenExit = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8 3v3a2 2 0 0 1-2 2H3m18 0h-3a2 2 0 0 1-2-2V3m0 18v-3a2 2 0 0 1 2-2h3M3 16h3a2 2 0 0 1 2 2v3"></path></svg>';

  landscapeBtn.innerHTML = svgFullscreenEnter;

  const cornerRatio = 0.25;
  let hideTimeout = null;

  function isIOSDevice() {
    return /iPad|iPhone|iPod/.test(navigator.userAgent) && !window.MSStream;
  }

  function formatTime(sec){
    sec = Math.floor(sec) || 0;
    const h = Math.floor(sec/3600);
    const m = Math.floor((sec%3600)/60);
    const s = sec%60;
    if(h>0) return h + ":" + String(m).padStart(2,'0') + ":" + String(s).padStart(2,'0');
    return m + ":" + String(s).padStart(2,'0');
  }

  function isFullscreenCard(){ return document.fullscreenElement === card || document.webkitFullscreenElement === card; }

  function isWideVideo() {
    return video.videoWidth > 0 && video.videoHeight > 0 && (video.videoWidth / video.videoHeight) > 1.2;
  }

  function unmuteVideo() {
    if (video.muted) {
      video.muted = false;
    }
  }

  async function tryAutoplay(){
    try {
      video.muted = false;
      await video.play();
    } catch(e){
      try {
        video.muted = true;
        await video.play();
      } catch(_){}
    }
    updatePlayIcon();
  }

  function updatePlayIcon(){ insidePlay.textContent = video.paused ? '▶︎' : '❚❚'; }

  async function togglePlay(){
    unmuteVideo();
    try{ if(video.paused){ try{ video.muted = false; } catch(e){} await video.play(); } else video.pause(); } catch(e){} updatePlayIcon(); }
  insidePlay.addEventListener('click', (e)=>{ e.stopPropagation(); togglePlay(); resetHideTimer(); });

  insideForward.addEventListener('click', (e)=>{ e.stopPropagation(); unmuteVideo(); video.currentTime = Math.min(video.duration || 0, video.currentTime + 10); resetHideTimer(); });
  insideBack.addEventListener('click', (e)=>{ e.stopPropagation(); unmuteVideo(); video.currentTime = Math.max(0, video.currentTime - 10); resetHideTimer(); });

  let scrubbing = false;
  let wasPlayingBeforeScrub = false;
  function timeFromClientX(clientX){
    const r = seekBar.getBoundingClientRect();
    let p = (clientX - r.left) / r.width;
    p = Math.max(0, Math.min(1, p));
    return (video.duration || 0) * p;
  }
  function startScrub(clientX){
    unmuteVideo();
    wasPlayingBeforeScrub = !video.paused;
    try { video.pause(); } catch(e){}
    scrubbing = true;
    const t = timeFromClientX(clientX);
    video.currentTime = t;
    currentEl.textContent = formatTime(t);
    const pct = (t / (video.duration || 1)) * 100;
    fill.style.width = pct + '%';
    thumb.style.left = pct + '%';
    resetHideTimer();
  }
  function moveScrub(clientX){
    if(!scrubbing) return;
    const t = timeFromClientX(clientX);
    video.currentTime = t;
    currentEl.textContent = formatTime(t);
    const pct = (t / (video.duration || 1)) * 100;
    fill.style.width = pct + '%';
    thumb.style.left = pct + '%';
    resetHideTimer();
  }
  function endScrub(){
    if(!scrubbing) return;
    scrubbing = false;
    if(wasPlayingBeforeScrub){
      try { video.play().catch(()=>{}); } catch(e){}
    }
    resetHideTimer();
  }

  seekBar.addEventListener('mousedown', (e)=>{ e.preventDefault(); e.stopPropagation(); startScrub(e.clientX); });
  window.addEventListener('mousemove', (e)=>{ moveScrub(e.clientX); });
  window.addEventListener('mouseup', (e)=>{ endScrub(); });
  seekBar.addEventListener('touchstart', (e)=>{ e.preventDefault(); e.stopPropagation(); startScrub(e.touches[0].clientX); }, {passive:false});
  window.addEventListener('touchmove', (e)=>{ if(e.touches && e.touches[0]) moveScrub(e.touches[0].clientX); }, {passive:false});
  window.addEventListener('touchend', (e)=>{ endScrub(); });
  thumb.addEventListener('pointerdown', (e)=>{ e.preventDefault(); e.stopPropagation(); startScrub(e.clientX); });
  window.addEventListener('pointermove', (e)=>{ if(e.pointerType) moveScrub(e.clientX); });
  window.addEventListener('pointerup', (e)=>{ endScrub(); });

  video.addEventListener('timeupdate', ()=> {
    currentEl.textContent = formatTime(video.currentTime);
    const pct = (video.currentTime / (video.duration || 1)) * 100;
    fill.style.width = pct + '%';
    thumb.style.left = pct + '%';
    seekBar.setAttribute('aria-valuenow', Math.floor(video.currentTime || 0));
  });
  video.addEventListener('loadedmetadata', ()=> {
    durationEl.textContent = formatTime(video.duration || 0);
    seekBar.setAttribute('aria-valuemax', Math.floor(video.duration || 0));
  });
  video.addEventListener('play', updatePlayIcon);
  video.addEventListener('pause', updatePlayIcon);

  function showAll(){
    controlsWrap.classList.remove('controls-hidden');
    controlsWrap.classList.add('controls-visible');
    insideMini.classList.remove('inside-hidden');
    insideMini.classList.add('inside-visible');
    resetHideTimer();
  }
  function hideAll(){
    controlsWrap.classList.remove('controls-visible');
    controlsWrap.classList.add('controls-hidden');
    insideMini.classList.remove('inside-visible');
    insideMini.classList.add('inside-hidden');
    if(hideTimeout) clearTimeout(hideTimeout);
  }
  function toggleAll(){
    const isVisible = controlsWrap.classList.contains('controls-visible') && insideMini.classList.contains('inside-visible');
    if(isVisible) {
      hideAll();
    } else {
      showAll();
    }
  }

  function resetHideTimer() {
    if(hideTimeout) clearTimeout(hideTimeout);
    hideTimeout = setTimeout(() => {
      hideAll();
    }, 4000);
  }

  function isOverControls(target){
    return !!(target && (target.closest && (target.closest('.controls-wrap') || target.closest('.inside-mini-controls') || target.closest('.inside-item') || target.closest('.seek'))));
  }

  card.addEventListener('click', (e)=>{
    if(isOverControls(e.target)) return;
    hideSwipeIndicatorsDirectly();
    toggleAll();
  });

  window.addEventListener('mousemove', resetHideTimer);
  window.addEventListener('touchstart', resetHideTimer, {passive:true});

  [insidePlay, insideForward, insideBack, seekBar, landscapeBtn].forEach(el=>{
    if(!el) return;
    el.addEventListener('click', e=>e.stopPropagation());
    el.addEventListener('pointerdown', e=>e.stopPropagation());
    el.addEventListener('touchstart', e=>e.stopPropagation(), {passive:false});
  });

  async function enterLandscapeMode(){
    unmuteVideo();
    const wasPlaying = !video.paused;
    if (isIOSDevice()) {
      if (video.webkitEnterFullscreen) {
        try {
          await video.webkitEnterFullscreen();
        } catch (err) {}
      }
      return;
    }
    try{
      const el = card;
      if (el.requestFullscreen) await el.requestFullscreen();
      else if (el.webkitRequestFullscreen) await el.webkitRequestFullscreen();
      if (isWideVideo() && screen.orientation && screen.orientation.lock) {
        try { await screen.orientation.lock('landscape'); } catch(_) {}
      }
    }catch(err){}
    card.classList.add('landscape-mode');
    if(wasPlaying) {
        try { await video.play(); } catch(err){}
    }
    showAll();
  }

  async function exitLandscapeMode(){
    const wasPlaying = !video.paused;
    try{
      if (screen.orientation && screen.orientation.unlock) {
        try { screen.orientation.unlock(); } catch(_) {}
      }
      if (document.exitFullscreen) await document.exitFullscreen();
      else if (document.webkitExitFullscreen) await document.webkitExitFullscreen();
    }catch(err){}
    card.classList.remove('landscape-mode');
    if(wasPlaying) {
        try { await video.play(); } catch(err){}
    }
    showAll();
  }
  
  landscapeBtn.addEventListener('click', async (e)=>{ e.stopPropagation(); if(!isFullscreenCard()){ await enterLandscapeMode(); } else { await exitLandscapeMode(); } });

  document.addEventListener('fullscreenchange', ()=> {
    if(document.fullscreenElement === card) {
      card.classList.add('landscape-mode');
      landscapeBtn.innerHTML = svgFullscreenExit;
      showAll();
    } else {
      card.classList.remove('landscape-mode');
      landscapeBtn.innerHTML = svgFullscreenEnter;
      showAll();
    }
  });

  let gesture = null;
  let indicatorTimeoutLeft = null;
  let indicatorTimeoutRight = null;
  let startTouchX = 0;
  let startTouchY = 0;
  let isVerticalSwipe = false;
  let swipeDirectionDetermined = false;

  function showSwipeIndicator(kind, percent){
    if (kind === 'brightness') {
      swipeIconLeft.innerHTML = svgBrightness;
      swipeValueLeft.textContent = percent + '%';
      swipeIndicatorLeft.classList.add('show');
      if(indicatorTimeoutLeft) clearTimeout(indicatorTimeoutLeft);
      indicatorTimeoutLeft = setTimeout(()=>{ swipeIndicatorLeft.classList.remove('show'); }, 800);
    } else {
      swipeIconRight.innerHTML = svgVolume;
      swipeValueRight.textContent = percent + '%';
      swipeIndicatorRight.classList.add('show');
      if(indicatorTimeoutRight) clearTimeout(indicatorTimeoutRight);
      indicatorTimeoutRight = setTimeout(()=>{ swipeIndicatorRight.classList.remove('show'); }, 800);
    }
  }

  function hideSwipeIndicatorsDirectly() {
    swipeIndicatorLeft.classList.remove('show');
    swipeIndicatorRight.classList.remove('show');
    if(indicatorTimeoutLeft) clearTimeout(indicatorTimeoutLeft);
    if(indicatorTimeoutRight) clearTimeout(indicatorTimeoutRight);
  }

  function clamp(v,a=0,b=1){ return Math.max(a, Math.min(b, v)); }
  
  function startLandscapeGesture(clientX, clientY){
    if(!isFullscreenCard()) return;
    unmuteVideo();
    const rect = card.getBoundingClientRect();
    const relX = clientX - rect.left;
    const limit = rect.width * cornerRatio;
    if(relX <= limit){
      const styleFilter = getComputedStyle(video).filter || '';
      const match = styleFilter.match(/brightness\(([^)]+)\)/);
      const initial = match ? parseFloat(match[1]) : 1;
      gesture = { type:'brightness', startY:clientY, initialValue: isNaN(initial) ? 1 : initial };
      showSwipeIndicator('brightness', Math.round(gesture.initialValue * 100));
    } else if(relX >= (rect.width - limit)){
      gesture = { type:'volume', startY:clientY, initialValue: video.volume };
      showSwipeIndicator('volume', Math.round(gesture.initialValue * 100));
    } else { gesture = null; }
  }

  function moveLandscapeGesture(clientY){
    if(!gesture) return;
    const rect = card.getBoundingClientRect();
    const delta = (gesture.startY - clientY);
    const pct = delta / (rect.height * 0.7);
    let newVal = gesture.initialValue + pct;
    newVal = clamp(newVal, 0, 1);
    if(gesture.type === 'brightness'){
      const applied = Math.max(0.05, newVal);
      video.style.filter = 'brightness(' + applied + ')';
      showSwipeIndicator('brightness', Math.round(applied * 100));
    } else {
      video.volume = newVal;
      showSwipeIndicator('volume', Math.round(newVal * 100));
    }
  }

  function endLandscapeGesture(){
    if(!gesture) return;
    gesture = null;
  }

  card.addEventListener('touchstart', (e)=>{
    unmuteVideo();
    const t = e.touches[0];
    startTouchX = t.clientX;
    startTouchY = t.clientY;
    isVerticalSwipe = false;
    swipeDirectionDetermined = false;
  }, {passive:true});

  card.addEventListener('touchmove', (e)=>{
    if (!e.touches.length) return;
    const t = e.touches[0];
    const diffX = t.clientX - startTouchX;
    const diffY = t.clientY - startTouchY;

    if (!swipeDirectionDetermined) {
      if (Math.abs(diffY) > 10 || Math.abs(diffX) > 10) {
        swipeDirectionDetermined = true;
        if (Math.abs(diffY) > Math.abs(diffX)) {
          isVerticalSwipe = true;
          startLandscapeGesture(startTouchX, startTouchY);
        }
      }
    }

    if (isVerticalSwipe) {
      if (e.cancelable) e.preventDefault();
      moveLandscapeGesture(t.clientY);
    }
  }, {passive:false});

  card.addEventListener('touchend', ()=>{
    endLandscapeGesture();
  }, {passive:true});

  function showBuffering() {
    backContainer.style.display = 'none';
    playContainer.style.display = 'none';
    forwardContainer.style.display = 'none';
    spinnerContainer.style.display = 'flex';
  }

  function hideBuffering() {
    backContainer.style.display = 'flex';
    playContainer.style.display = 'flex';
    forwardContainer.style.display = 'flex';
    spinnerContainer.style.display = 'none';
  }

  video.addEventListener('loadstart', showBuffering);
  video.addEventListener('waiting', showBuffering);
  video.addEventListener('seeking', showBuffering);
  video.addEventListener('playing', hideBuffering);
  video.addEventListener('seeked', hideBuffering);
  video.addEventListener('canplay', hideBuffering);
  video.addEventListener('pause', hideBuffering);
  
  if (video.readyState >= 3) {
      hideBuffering();
  }

  const sourceUrl = video.querySelector('source')?.src || '';
  video.addEventListener('error', async (e)=>{
    try {
      const r = await fetch(sourceUrl, { method: 'HEAD', mode: 'cors' });
      if(!r.ok) showError('Sève an repon ' + r.status + '.');
      else showError('Fòma videyo sa a pa sipòte.');
    } catch(fetchErr){
      showError('Erè koneksyon (oubyen rezo a pa la).');
    }
  });

  function showError(msg){ 
      if(errTxt) errTxt.textContent = msg; 
      if(errorOverlay) errorOverlay.style.display='flex'; 
      hideBuffering();
  }

  document.addEventListener('click', unmuteVideo, {once: false});
  document.addEventListener('touchstart', unmuteVideo, {once: false});

  tryAutoplay();
  showAll();

  window.__player = { video };
})();
</script>
</body>
</html>`;
}

function sendUnknown(req, res) {
    if (req.headers.accept && req.headers.accept.includes('text/html')) {
        res.status(404).send('<!doctype html><html lang="ht"><head><meta charset="UTF-8"><title>Paj sa pa ekziste</title></head><body style="background:#000;color:#fff;text-align:center;padding:50px;font-family:sans-serif;"><h1>Paj sa pa ekziste</h1><script>setTimeout(function(){ window.close(); window.history.back(); }, 1500);</script></body></html>');
    } else {
        res.status(404).send('Paj sa pa ekziste');
    }
}

async function getRemoteObjectMeta(token) {
    try {
        const head = await s3.send(new HeadObjectCommand({
            Bucket: R2_BUCKET,
            Key: token
        }));
        return {
            exists: true,
            contentType: head.ContentType || null,
            contentLength: head.ContentLength || null,
            metadata: head.Metadata || {},
            lastModified: head.LastModified ? new Date(head.LastModified).toISOString() : null
        };
    } catch (err) {
        return null;
    }
}

function safeDecodeURIComponent(value) {
    try {
        return decodeURIComponent(value);
    } catch (err) {
        return value;
    }
}

async function ensureMappingFromR2(token, fallbackName) {
    if (mappings[token]) return mappings[token];
    const meta = await getRemoteObjectMeta(token);
    if (!meta || !meta.exists) return null;

    const originalName = safeDecodeURIComponent((meta.metadata && meta.metadata.originalname) || fallbackName || token) || fallbackName || token;
    const safeOriginal = safeFileName((meta.metadata && meta.metadata.safeoriginal) || originalName || token);

    const entry = {
        token,
        originalName,
        safeOriginal,
        size: meta.contentLength || null,
        mime: meta.contentType || null,
        createdAt: meta.lastModified || new Date().toISOString(),
        storage: 'r2'
    };

    mappings[token] = entry;
    saveMappingsToDisk();
    return entry;
}

function buildRemoteUrl(remotePath) {
    return `${R2_BASE_URL}/${encodeURIComponent(String(remotePath))}`;
}

function applyDownloadHeaders(res, filename, inlinePreferred, mime, contentLength, contentRange, acceptRanges) {
    const type = mime || contentTypeFromName(filename);
    res.setHeader('Content-Type', type);
    res.setHeader('Accept-Ranges', acceptRanges || 'bytes');
    if (contentLength) res.setHeader('Content-Length', contentLength);
    if (contentRange) res.setHeader('Content-Range', contentRange);
    const dispositionType = inlinePreferred ? 'inline' : 'attachment';
    res.setHeader('Content-Disposition', `${dispositionType}; filename="${safeFileName(filename)}"`);
}

async function serveRemoteRawFile(req, res, remotePath, filename, options = {}) {
    const remoteUrl = buildRemoteUrl(remotePath);
    const headers = {};
    if (req.headers.range) headers.Range = req.headers.range;

    const fetchMethod = req.method === 'HEAD' ? 'HEAD' : 'GET';
    let upstream;
    try {
        upstream = await fetch(remoteUrl, { method: fetchMethod, headers, redirect: 'follow' });
    } catch (err) {
        return sendUnknown(req, res);
    }
    
    if (!upstream.ok && upstream.status !== 206) return sendUnknown(req, res);

    const contentType = upstream.headers.get('content-type') || contentTypeFromName(filename);
    const contentLength = upstream.headers.get('content-length');
    const acceptRanges = upstream.headers.get('accept-ranges') || 'bytes';
    const contentRange = upstream.headers.get('content-range');
    const inlinePreferred = Boolean(options.inlinePreferred);

    res.status(upstream.status === 206 ? 206 : 200);
    applyDownloadHeaders(res, filename, inlinePreferred, contentType, contentLength, contentRange, acceptRanges);

    if (req.method === 'HEAD') {
        return res.end();
    }

    if (!upstream.body) return res.end();
    const body = Readable.fromWeb(upstream.body);
    body.on('error', () => {
        if (!res.writableEnded) res.end();
    });
    body.pipe(res);
}

const app = express();

app.disable('x-powered-by');
app.use(cors());

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Range');
    res.header('Access-Control-Expose-Headers', 'Content-Range, Accept-Ranges, Content-Length, Content-Type, Content-Disposition');
    next();
});

app.use(compression({
    filter: (req, res) => {
        try {
            if (req && req.path && req.path.startsWith('/TF-')) return false;
        } catch (e) {}
        return compression.filter(req, res);
    }
}));

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'), { index: 'index.html' }));

const multerStorage = {
    _handleFile: async function(req, file, cb) {
        const token = genToken();
        const originalName = file.originalname || 'file';
        const safeOriginal = safeFileName(originalName);

        req.uploadToken = token;
        req.safeOriginal = safeOriginal;
        req.originalName = originalName;

        let size = 0;
        file.stream.on('data', chunk => { size += chunk.length; });

        try {
            const parallelUploads3 = new Upload({
                client: s3,
                params: {
                    Bucket: R2_BUCKET,
                    Key: token,
                    Body: file.stream,
                    ContentType: file.mimetype || contentTypeFromName(originalName),
                    Metadata: {
                        originalname: encodeURIComponent(originalName),
                        safeoriginal: safeOriginal,
                        token
                    }
                },
                queueSize: 2,
                partSize: 8 * 1024 * 1024
            });

            await parallelUploads3.done();

            cb(null, {
                size: size,
                mimetype: file.mimetype || contentTypeFromName(originalName)
            });
        } catch (err) {
            cb(err);
        }
    },
    _removeFile: function(req, file, cb) {
        cb(null);
    }
};

const upload = multer({ storage: multerStorage, limits: { fileSize: MAX_FILE_SIZE } });

app.post('/upload', upload.single('file'), async (req, res) => {
    try {
        if (!req.file && !req.uploadToken) return res.status(400).json({ error: 'Fichye pa la' });

        const token = req.uploadToken;
        const originalName = req.originalName || (req.file && req.file.originalname) || 'file';
        const safeOriginal = req.safeOriginal || safeFileName(originalName);
        const entry = {
            token,
            originalName,
            safeOriginal,
            size: req.file ? req.file.size : 0,
            mime: req.file ? req.file.mimetype : contentTypeFromName(originalName),
            createdAt: new Date().toISOString(),
            storage: 'r2'
        };

        mappings[token] = entry;
        saveMappingsToDisk();

        const origin = (process.env.BASE_URL || 'https://bref.adamdh7.org').replace(/\/+$/, '');
        const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;
        return res.json({ token, url: `${origin}${sharePath}`, sharePath, info: entry });
    } catch (err) {
        return res.status(500).json({ error: 'Erè souple eseye ankò' });
    }
});

app.get(['/TF-:token', '/TF-:token/', '/TF-:token/:name'], async (req, res) => {
    try {
        let token = req.params.token;
        if (token) {
            token = token.replace(/\/$/, '');
        }
        const requestedName = req.params.name || null;
        const entry = await ensureMappingFromR2(token, requestedName);

        if (!entry) return sendUnknown(req, res);

        const filename = requestedName || entry.safeOriginal || entry.originalName || 'file';
        const isVideo = isVideoFile(filename);
        const isImage = isImageFile(filename);
        const previewable = isImage || isVideo;

        const rawRequested = req.query.raw === '1';
        const downloadRequested = req.query.download === '1';
        const htmlPreview = wantsHtmlPreview(req);

        if (!requestedName) {
            if (isVideo) {
                const origin = (process.env.BASE_URL || 'https://bref.adamdh7.org').replace(/\/+$/, '');
                const targetUrl = `/TF-${token}/${encodeURIComponent(filename)}?raw=1`;
                const fullUrl = `${origin}/TF-${token}`;
                return res.status(200).type('html').send(buildCustomPlayerHtml(filename, targetUrl, fullUrl, entry.mime));
            }
            if (isImage) {
                const targetUrl = `/TF-${token}/${encodeURIComponent(filename)}?raw=1`;
                return res.status(200).type('html').send(buildViewerHtml(filename, targetUrl, filename));
            }
            return res.status(403).send('<!doctype html><html lang="ht"><head><meta charset="UTF-8"><title>Aksè refize</title></head><body style="background:#000;color:#fff;text-align:center;padding:50px;font-family:sans-serif;"><h1>Aksè refize san non fichye a</h1></body></html>');
        }

        if (htmlPreview && !downloadRequested && !rawRequested) {
            if (isVideo) {
                const origin = (process.env.BASE_URL || 'https://bref.adamdh7.org').replace(/\/+$/, '');
                const targetUrl = `/TF-${token}/${encodeURIComponent(filename)}?raw=1`;
                const fullUrl = `${origin}/TF-${token}/${encodeURIComponent(filename)}`;
                return res.status(200).type('html').send(buildCustomPlayerHtml(filename, targetUrl, fullUrl, entry.mime));
            }
            if (isImage) {
                const targetUrl = `/TF-${token}/${encodeURIComponent(filename)}?raw=1`;
                return res.status(200).type('html').send(buildViewerHtml(filename, targetUrl, filename));
            }
        }

        const inlinePreferred = (previewable || rawRequested) && !downloadRequested;
        return serveRemoteRawFile(req, res, token, filename, { inlinePreferred });
    } catch (err) {
        return sendUnknown(req, res);
    }
});

app.get('/_admin/mappings', (req, res) => {
    return res.json({ count: Object.keys(mappings).length, tokens: Object.keys(mappings).slice(0, 50) });
});

app.get('/sitemap.xml', (req, res) => {
    res.sendFile(path.join(__dirname, 'sitemap.xml'));
});

app.get('/poste.json', (req, res) => {
    try {
        const filePath = path.join(__dirname, 'poste.json');
        if (!fs.existsSync(filePath)) return res.json([]);
        const fileContent = fs.readFileSync(filePath, 'utf8');
        const jsonData = JSON.parse(fileContent);
        if (!Array.isArray(jsonData)) return res.json(jsonData);
        const shuffled = [...jsonData].sort(() => 0.5 - Math.random());
        const randomCount = Math.floor(Math.random() * 2) + 3;
        res.json(shuffled.slice(0, randomCount));
    } catch (err) {
        res.status(500).json({ error: 'Server Error' });
    }
});

app.get('/health', (req, res) => res.json({ ok: true }));

app.get('*', (req, res, next) => {
    if (req.path.startsWith('/TF-') || req.path.startsWith('/upload') || req.path.startsWith('/_admin') || req.path.startsWith('/health')) return next();
    const indexPath = path.join(__dirname, 'public', 'index.html');
    if (fs.existsSync(indexPath)) {
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        return res.sendFile(indexPath);
    }
    return res.status(404).send('Paj sa pa ekziste');
});

app.use((err, req, res, next) => {
    if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'Fichye a twò gwo. Max: 7Go' });
    if (err) return res.status(500).json({ error: 'Erè nan sève a' });
    next();
});

setInterval(async () => {
    const now = Date.now();
    for (const [token, entry] of Object.entries(mappings)) {
        const created = new Date(entry.createdAt).getTime();
        if (Number.isFinite(created) && now - created > MAX_AGE_MS) {
            try {
                await s3.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: token }));
                delete mappings[token];
                saveMappingsToDisk();
            } catch (e) {}
        }
    }
}, 3600000);

app.listen(PORT);
