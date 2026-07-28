# PyHost — Telegram Bot Hosting Panel

## Deploy on Render (Free)

### 1. Upload to GitHub
```bash
git init
git add .
git commit -m "init"
git remote add origin https://github.com/YOUR_USER/pyhost.git
git push -u origin main
```

### 2. Render Setup
- Go to [render.com](https://render.com) → **New → Web Service**
- Connect your GitHub repo
- Settings:
  - **Runtime:** Python 3
  - **Build Command:** `pip install -r requirements.txt`
  - **Start Command:** `gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120`

### 3. Environment Variables (on Render dashboard)
| Key | Value |
|-----|-------|
| `SECRET_KEY` | any random string |
| `ADMIN_USER` | admin |
| `ADMIN_PASS` | your_password |

### 4. Done
Panel runs at `https://your-app.onrender.com`

---

## Features
- Upload `.py` or `.zip` bot files
- Start / Stop / Restart bots as real processes
- Live streaming logs via SSE
- File editor in browser
- Package installer (pip) with live output
- Environment variable manager per bot
- Auto-restart watchdog on crash
- Change password from settings

## Default Login
- **Username:** admin  
- **Password:** admin123

> ⚠️ Change the password after first login!
