"""Builds report figures from results/ (run after run_all.py).   python make_figures.py"""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt                     # noqa: E402
from matplotlib.patches import FancyBboxPatch       # noqa: E402
from PIL import Image, ImageDraw, ImageFont         # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
FIG = os.path.join(ROOT, "figures")
os.makedirs(FIG, exist_ok=True)
M = json.load(open(os.path.join(ROOT, "results", "metrics.json"), encoding="utf-8"))

# ---------------------------------------------------------------- 1. ablation chart
modes = ["A\nBaseline", "B\nExceptions", "C\n+ Retry", "D\n+ Checkpoint\n/Rollback"]
keys = ["A", "B", "C", "D"]
succ = [M[k]["successful"] for k in keys]
amt = [M[k]["processed_amount_kzt"] / 1000 for k in keys]
fig, ax = plt.subplots(1, 2, figsize=(10, 3.6), dpi=160)
c = ["#9aa5b1", "#5b8def", "#2f6fd6", "#1b4fa8"]
b1 = ax[0].bar(modes, succ, color=c)
ax[0].set_title("Successful transactions (of 15)", fontsize=10)
ax[0].set_ylim(0, 16)
ax[0].bar_label(b1, fontsize=9)
b2 = ax[1].bar(modes, amt, color=c)
ax[1].set_title("Processed amount, thousand KZT (of 437)", fontsize=10)
ax[1].set_ylim(0, 480)
ax[1].bar_label(b2, fmt="%.0f", fontsize=9)
for a in ax:
    a.spines[["top", "right"]].set_visible(False)
    a.tick_params(labelsize=8)
cons = ["consistent", "2 orphan rows", "2 orphan rows", "consistent"]
for i, t in enumerate(cons):
    ax[0].text(i, -4.2, t, ha="center", fontsize=7, color="#b03030" if "orphan" in t else "#2e7d32")
fig.suptitle(f"Contribution of each mechanism (run commit {M['meta']['commit']})", fontsize=10)
fig.tight_layout()
fig.savefig(os.path.join(FIG, "fig_ablation.png"))
plt.close(fig)

# ---------------------------------------------------------------- 2. architecture / recovery flow
fig, ax = plt.subplots(figsize=(10, 4.3), dpi=160)
ax.set_xlim(0, 100)
ax.set_ylim(0, 44)
ax.axis("off")


def box(x, y, w, h, text, fc="#e8f0fe", ec="#2f6fd6", fs=8):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4", fc=fc, ec=ec, lw=1.2))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fs)


def arrow(x1, y1, x2, y2, t="", col="#333"):
    ax.annotate("", (x2, y2), (x1, y1), arrowprops=dict(arrowstyle="->", color=col, lw=1.1))
    if t:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 0.8, t, fontsize=6.5, ha="center", color=col)


box(1, 30, 13, 8, "15 transactions\nft/data.py")
box(19, 30, 12, 8, "Validate\n(ValidationError\n= permanent)")
box(35, 30, 12, 8, "Process\nNetwork / Timeout\nfaults injected")
box(51, 30, 14, 8, "Record\n1) insert row\n2) apply totals\nDB fault between")
box(70, 30, 13, 8, "Ledger\nidempotent on\ntxn_id", fc="#e6f4ea", ec="#2e7d32")
box(87, 30, 12, 8, "CheckpointManager\nCP every 5 OK\nSHA-256 + journal", fc="#e6f4ea", ec="#2e7d32", fs=7)
arrow(14.8, 34, 18.6, 34)
arrow(31.8, 34, 34.6, 34)
arrow(47.8, 34, 50.6, 34)
arrow(65.8, 34, 69.6, 34)
arrow(83.8, 34, 86.6, 34)
box(28, 12, 20, 9, "except TransactionError\nclassify -> log JSON\n(Part B)", fc="#fff4e5", ec="#e08a00")
box(55, 12, 20, 9, "RetryPolicy (Part C)\nmax 2 retries\nbackoff 0.1 s, 0.2 s", fc="#fff4e5", ec="#e08a00")
box(80, 12, 18, 9, "Rollback (Part D)\nrestore latest valid CP\n+ replay journal", fc="#fdecea", ec="#c62828")
box(2, 12, 20, 9, "Continue with\nnext transaction", fc="#f3e8fd", ec="#7b3fb5")
arrow(41, 29.5, 38, 21.6, "exception", "#e08a00")
arrow(57, 29.5, 42, 21.6, "", "#e08a00")
arrow(48.6, 16.5, 54.4, 16.5, "transient", "#e08a00")
arrow(75.6, 16.5, 79.4, 16.5, "DB, exhausted", "#c62828")
arrow(65, 21.6, 41, 29.5, "retry", "#2f6fd6")
arrow(27.4, 16.5, 22.6, 16.5, "", "#7b3fb5")
ax.annotate("", (12, 21.6), (89, 11.4), arrowprops=dict(arrowstyle="->", color="#7b3fb5", lw=1,
                                                         connectionstyle="arc3,rad=-0.25"))
ax.text(50, 3, "Solid path = normal Validate -> Process -> Record.  Orange = exception handling + retry.  "
        "Red = rollback on exhausted Database failure.", ha="center", fontsize=7, color="#555")
ax.set_title("Implementation architecture and recovery paths", fontsize=10)
fig.tight_layout()
fig.savefig(os.path.join(FIG, "fig_architecture.png"))
plt.close(fig)

# ---------------------------------------------------------------- 3. terminal "screenshot" of the real run
lines = open(os.path.join(ROOT, "results", "console_output.txt"), encoding="utf-8").read().splitlines()
start = next(i for i, l in enumerate(lines) if l.startswith("PART D"))
chunk = [l[:175] for l in lines[start - 1:]]
chunk = ["$ python run_all.py   (excerpt: Part D + Part E)"] + chunk
font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", 13)
W, LH = 1440, 18
img = Image.new("RGB", (W, LH * len(chunk) + 20), "#1e1e1e")
d = ImageDraw.Draw(img)
for i, l in enumerate(chunk):
    col = "#d4d4d4"
    if "WARNING" in l:
        col = "#e5c07b"
    if "ERROR" in l or "CRITICAL" in l:
        col = "#f28b82"
    if "checkpoint" in l or l.startswith("$"):
        col = "#8ab4f8"
    if "run_end" in l:
        col = "#81c995"
    d.text((10, 10 + i * LH), l, font=font, fill=col)
img.save(os.path.join(FIG, "fig_run_screenshot.png"))
print("figures written to", FIG)
