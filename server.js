const express = require('express');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const compression = require('compression');
const multer = require('multer');
const { spawn, spawnSync } = require('child_process');
const { Readable } = require('stream');
const { S3Client, PutObjectCommand, DeleteObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');

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

const FFMPEG_AVAILABLE = (() => {
    try {
        const result = spawnSync('ffmpeg', ['-version'], { stdio: 'ignore' });
        return !result.error && result.status === 0;
    } catch {
        return false;
    }
})();

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
        '.otf': 'font/otf'
    };
    return map[ext] || 'application/octet-stream';
}

function isImageFile(filename) {
    return ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.svg', '.avif', '.heic', '.heif', '.tiff', '.ico'].includes(path.extname(filename).toLowerCase());
}

function isAudioFile(filename) {
    return ['.mp3', '.wav', '.ogg', '.m4a', '.flac', '.aac', '.opus', '.mid', '.midi'].includes(path.extname(filename).toLowerCase());
}

function isPdfFile(filename) {
    return path.extname(filename).toLowerCase() === '.pdf';
}

function isDirectVideoFile(filename) {
    return ['.mp4', '.webm', '.m4v', '.mov', '.ogg', '.ogv', '.quicktime'].includes(path.extname(filename).toLowerCase());
}

function needsTranscode(filename) {
    return ['.mkv', '.avi', '.wmv', '.flv', '.3gp', '.ts', '.mpeg', '.mpg', '.m2ts'].includes(path.extname(filename).toLowerCase());
}

function getDisplayName(filename) {
    return path.basename(filename, path.extname(filename)).replace(/[-_]+/g, ' ').replace(/\s+/g, ' ').trim() || 'Mizik';
}

function buildViewerHtml(title, mediaUrl, filename) {
    const safeTitle = String(title).replace(/</g, '&lt;').replace(/>/g, '&gt;');
    let mediaBlock = '';

    if (isImageFile(filename)) {
        mediaBlock = `<img src="${mediaUrl}" alt="${safeTitle}" style="display:block;max-width:100vw;max-height:100vh;width:auto;height:auto;object-fit:contain;" />`;
    } else if (isDirectVideoFile(filename) || needsTranscode(filename)) {
        mediaBlock = `<video src="${mediaUrl}" controls autoplay playsinline preload="auto" style="display:block;max-width:100vw;max-height:100vh;width:auto;height:auto;object-fit:contain;background:#000;"></video>`;
    } else if (isAudioFile(filename)) {
        mediaBlock = `<audio src="${mediaUrl}" controls autoplay preload="auto" style="display:block;max-width:min(92vw,900px);width:100%;height:auto;"></audio>`;
    } else if (isPdfFile(filename)) {
        mediaBlock = `<iframe src="${mediaUrl}" style="display:block;width:min(100vw,1200px);height:100vh;border:0;background:#000;"></iframe>`;
    } else {
        mediaBlock = `<a href="${mediaUrl}" style="color:#fff;font-family:Arial,sans-serif;word-break:break-all;text-decoration:none;font-size:18px;">${mediaUrl}</a>`;
    }

    return `<!doctype html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${safeTitle}</title>
<link rel="icon" type="image/png" href="${ICON_URL}">
<link rel="shortcut icon" type="image/png" href="${ICON_URL}">
<link rel="apple-touch-icon" href="${ICON_URL}">
<meta name="theme-color" content="#000000">
<style>
html, body { margin: 0; width: 100%; height: 100%; overflow: hidden; background: #000; }
body { display: flex; align-items: center; justify-content: center; }
#wrap { width: 100vw; height: 100vh; display: flex; align-items: center; justify-content: center; }
#wrap > * { max-width: 100vw; max-height: 100vh; }
</style>
</head>
<body><div id="wrap">${mediaBlock}</div></body></html>`;
}

function sendUnknown(req, res) {
    if (req.headers.accept && req.headers.accept.includes('text/html')) {
        res.status(404).send('<!doctype html><html lang="fr"><head><meta charset="UTF-8"><title>Inconnu</title></head><body style="background:#000;color:#fff;text-align:center;padding:50px;font-family:sans-serif;"><h1>Inconnu</h1><script>setTimeout(function(){ window.close(); window.history.back(); }, 1500);</script></body></html>');
    } else {
        res.status(404).send('Inconnu');
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

async function serveRemoteRawFile(req, res, remotePath, filename) {
    const remoteUrl = `${R2_BASE_URL}/${encodeURIComponent(String(remotePath))}`;
    const headers = {};
    if (req.headers.range) headers.Range = req.headers.range;

    const fetchMethod = req.method === 'HEAD' ? 'HEAD' : 'GET';

    const upstream = await fetch(remoteUrl, { method: fetchMethod, headers, redirect: 'follow' });
    if (!upstream.ok && upstream.status !== 206) return sendUnknown(req, res);

    const contentType = upstream.headers.get('content-type') || contentTypeFromName(filename);
    const contentLength = upstream.headers.get('content-length');
    const acceptRanges = upstream.headers.get('accept-ranges') || 'bytes';
    const contentRange = upstream.headers.get('content-range');

    res.status(upstream.status === 206 ? 206 : 200);
    res.setHeader('Content-Type', contentType);
    res.setHeader('Accept-Ranges', acceptRanges);
    if (contentLength) res.setHeader('Content-Length', contentLength);
    if (contentRange) res.setHeader('Content-Range', contentRange);

    if (req.method === 'HEAD') {
        return res.end();
    }

    if (!upstream.body) return res.end();
    const body = Readable.fromWeb(upstream.body);
    body.pipe(res);
}

function reqLikeCleanup(inputStream, ffmpeg, res, abort) {
    const stop = () => {
        abort();
        try { inputStream.destroy(); } catch {}
        try { ffmpeg.stdin.destroy(); } catch {}
    };
    res.on('close', stop);
    res.on('finish', stop);
    inputStream.on('error', stop);
}

function transcodeVideoStreamToMp4(inputStream, res) {
    if (!FFMPEG_AVAILABLE) {
        res.status(415).type('text/plain').send('ffmpeg manquant');
        return;
    }

    res.status(200);
    res.setHeader('Content-Type', 'video/mp4');
    res.setHeader('Accept-Ranges', 'none');

    const ffmpeg = spawn('ffmpeg', [
        '-hide_banner', '-loglevel', 'error', '-i', 'pipe:0',
        '-movflags', 'frag_keyframe+empty_moov+default_base_moof', '-f', 'mp4', 'pipe:1'
    ], { stdio: ['pipe', 'pipe', 'pipe'] });

    const abort = () => {
        try { ffmpeg.kill('SIGKILL'); } catch {}
    };

    reqLikeCleanup(inputStream, ffmpeg, res, abort);

    inputStream.pipe(ffmpeg.stdin);
    ffmpeg.stdout.pipe(res);

    ffmpeg.on('close', code => {
        if (code !== 0 && !res.headersSent) {
            res.status(415).type('text/plain').send('Erreur conversion');
        } else if (code !== 0 && !res.writableEnded) {
            res.end();
        }
    });

    ffmpeg.on('error', () => {
        if (!res.headersSent) res.status(500).type('text/plain').send('Erreur ffmpeg');
    });
}

async function serveRemoteVideoTranscode(req, res, remotePath) {
    const remoteUrl = `${R2_BASE_URL}/${encodeURIComponent(String(remotePath))}`;
    const fetchMethod = req.method === 'HEAD' ? 'HEAD' : 'GET';
    const upstream = await fetch(remoteUrl, { method: fetchMethod, redirect: 'follow' });
    if (!upstream.ok || !upstream.body) return sendUnknown(req, res);

    if (req.method === 'HEAD') {
        res.status(200);
        res.setHeader('Content-Type', 'video/mp4');
        res.setHeader('Accept-Ranges', 'none');
        return res.end();
    }

    const inputStream = Readable.fromWeb(upstream.body);
    transcodeVideoStreamToMp4(inputStream, res);
}

const app = express();

app.use(cors());

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Range');
    res.header('Access-Control-Expose-Headers', 'Content-Range, Accept-Ranges, Content-Length, Content-Type');
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
                    ContentType: file.mimetype,
                    Metadata: {
                        originalname: encodeURIComponent(originalName),
                        safeoriginal: safeOriginal,
                        token
                    }
                },
                queueSize: 4,
                partSize: 5 * 1024 * 1024
            });

            await parallelUploads3.done();

            cb(null, {
                size: size,
                mimetype: file.mimetype
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
        if (!req.file && !req.uploadToken) return res.status(400).json({ error: 'Fichier manquant' });

        const token = req.uploadToken;
        const originalName = req.originalName;
        const safeOriginal = req.safeOriginal;
        const entry = {
            token,
            originalName,
            safeOriginal,
            size: req.file ? req.file.size : 0,
            mime: req.file ? req.file.mimetype : 'application/octet-stream',
            createdAt: new Date().toISOString(),
            storage: 'r2'
        };

        mappings[token] = entry;
        saveMappingsToDisk();

        const origin = (process.env.BASE_URL || 'https://bref.adamdh7.org').replace(/\/+$/, '');
        const sharePath = `/TF-${token}/${encodeURIComponent(safeOriginal)}`;
        return res.json({ token, url: `${origin}${sharePath}`, sharePath, info: entry });
    } catch (err) {
        return res.status(500).json({ error: 'Erreur upload S3' });
    }
});

app.get(['/TF-:token', '/TF-:token/:name'], async (req, res) => {
    try {
        const token = req.params.token;
        const requestedName = req.params.name || null;
        const entry = await ensureMappingFromR2(token, requestedName);

        if (!entry) return sendUnknown(req, res);

        const filename = requestedName || entry.safeOriginal || entry.originalName || 'Mizik';
        let wantsRaw = req.query.raw === '1';
        let wantsTranscode = req.query.transcode === '1';

        const isBrowserDoc = req.headers.accept && req.headers.accept.includes('text/html') && !['image', 'video', 'audio'].includes(req.headers['sec-fetch-dest']);

        if (!isBrowserDoc && !wantsTranscode) wantsRaw = true;

        if (!wantsRaw && !wantsTranscode) {
            const displayName = getDisplayName(filename);
            const mediaMode = needsTranscode(filename) && FFMPEG_AVAILABLE ? 'transcode' : 'raw';
            const mediaUrl = `/TF-${token}/${encodeURIComponent(filename)}?${mediaMode}=1`;
            return res.status(200).type('html').send(buildViewerHtml(displayName, mediaUrl, filename));
        }

        if (wantsTranscode && needsTranscode(filename) && FFMPEG_AVAILABLE) {
            return serveRemoteVideoTranscode(req, res, token);
        }

        return serveRemoteRawFile(req, res, token, filename);
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
        const shuffled = jsonData.sort(() => 0.5 - Math.random());
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
    return res.status(404).send('Inconnu');
});

app.use((err, req, res, next) => {
    if (err && err.code === 'LIMIT_FILE_SIZE') return res.status(413).json({ error: 'Fichier trop grand. Max: 7Go' });
    if (err) return res.status(500).json({ error: 'Erreur serveur' });
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
