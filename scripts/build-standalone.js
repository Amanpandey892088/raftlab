// Inlines css + js into one self-contained HTML file: dist/raftlab-standalone.html
const fs = require('fs'), path = require('path'), root = path.join(__dirname, '..');
let h = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
h = h.replace('<link rel="stylesheet" href="css/style.css">', () => '<style>' + fs.readFileSync(path.join(root, 'css/style.css'), 'utf8') + '</style>');
h = h.replace(/<script src="([^"]+)"><\/script>/g, (_, p) => '<script>' + fs.readFileSync(path.join(root, p), 'utf8') + '</script>');
fs.mkdirSync(path.join(root, 'dist'), { recursive: true });
fs.writeFileSync(path.join(root, 'dist/raftlab-standalone.html'), h);
console.log('wrote dist/raftlab-standalone.html (' + h.length + ' bytes)');
