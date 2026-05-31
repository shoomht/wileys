const fs = require('fs')
const path = require('path')

const URL = 'https://wppconnect.io/whatsapp-versions/'

async function main() {
  try {
    const res = await fetch(URL, {
      headers: {
        'user-agent': 'Mozilla/5.0'
      }
    })

    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText}`)
    }

    const html = await res.text()

    const match = html.match(/Current Version[\s\S]{0,1200}?\b(\d+\.\d+\.\d+(?:-[a-zA-Z0-9.-]+)?)\b/i)

    if (!match) {
      console.log('Skip update WA Web version: versi tidak ditemukan')
      process.exit(0)
    }

    const rawWithSuffix = match[1].trim()

    // buang suffix seperti -alpha, -beta, dll
    const cleanRaw = rawWithSuffix.split('-')[0].trim()

    const version = cleanRaw.split('.').map(Number)

    if (version.length !== 3 || version.some(Number.isNaN)) {
      throw new Error(`Format versi tidak valid: ${cleanRaw}`)
    }

    // 1) update astrabail-version.json
    const jsonPath = path.join(__dirname, 'astrabail-version.json')
    const jsonData = {
      version,
      raw: cleanRaw
    }
    fs.writeFileSync(jsonPath, JSON.stringify(jsonData, null, 2) + '\n', 'utf8')

    // 2) update lib/Defaults/index.js
    const indexPath = path.join(__dirname, 'index.js')
    let indexContent = fs.readFileSync(indexPath, 'utf8')

    const newVersionArray = `[${version.join(', ')}]`

    const pattern = /exports\.version\s*=\s*waVer\?\.version\s*\|\|\s*\[[^\]]+\];?/

    if (!pattern.test(indexContent)) {
      throw new Error('Baris exports.version tidak ditemukan di lib/Defaults/index.js')
    }

    indexContent = indexContent.replace(
      pattern,
      `exports.version = waVer?.version || ${newVersionArray};`
    )

    fs.writeFileSync(indexPath, indexContent, 'utf8')

    console.log('WA version updated:', {
      version,
      raw: cleanRaw
    })
  } catch (err) {
    console.error('Gagal update WA version:', err.message)
    process.exit(1)
  }
}

main()


