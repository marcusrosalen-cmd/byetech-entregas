"""
Gera os ícones PNG da extensão (16x16, 48x48, 128x128).
Requer: pip install Pillow
"""
import os
import math

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "Pillow", "-q"])
    from PIL import Image, ImageDraw, ImageFont

SIZES = [16, 48, 128]
OUT   = os.path.join(os.path.dirname(__file__), "icons")
os.makedirs(OUT, exist_ok=True)

BG    = (59, 130, 246)   # #3b82f6 azul
FG    = (255, 255, 255)  # branco

def draw_icon(size):
    img  = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d    = ImageDraw.Draw(img)
    r    = size // 5  # raio dos cantos arredondados

    # Fundo arredondado
    d.rounded_rectangle([0, 0, size - 1, size - 1], radius=r, fill=BG)

    # Símbolo: duas setas formando um ciclo (simplificado como ↻)
    cx, cy = size / 2, size / 2
    ri = size * 0.28   # raio interno do arco
    lw = max(1, size // 10)  # espessura da linha

    # Arco superior (165° → 345°)
    bbox = [cx - ri, cy - ri, cx + ri, cy + ri]
    d.arc(bbox, start=160, end=350, fill=FG, width=lw)

    # Seta no fim do arco superior (ponta em ~350°)
    angle = math.radians(350)
    ax = cx + ri * math.cos(angle)
    ay = cy + ri * math.sin(angle)
    for da in [-30, 30]:
        a2 = math.radians(350 + 90 + da)
        bx = ax + lw * 2 * math.cos(a2)
        by = ay + lw * 2 * math.sin(a2)
        d.line([(ax, ay), (bx, by)], fill=FG, width=lw)

    # Arco inferior (345° → 165°, ou seja -15° → 160° revertido)
    d.arc(bbox, start=340, end=170, fill=FG, width=lw)

    # Seta no fim do arco inferior (ponta em ~170°)
    angle2 = math.radians(170)
    ax2 = cx + ri * math.cos(angle2)
    ay2 = cy + ri * math.sin(angle2)
    for da in [-30, 30]:
        a2 = math.radians(170 + 90 + da)
        bx = ax2 + lw * 2 * math.cos(a2)
        by = ay2 + lw * 2 * math.sin(a2)
        d.line([(ax2, ay2), (bx, by)], fill=FG, width=lw)

    return img

for s in SIZES:
    icon = draw_icon(s)
    path = os.path.join(OUT, f"icon{s}.png")
    icon.save(path, "PNG")
    print(f"  OK icons/icon{s}.png")

print("Icones gerados com sucesso.")
