import time
import tkinter as tk
import tester

seen = {}

def fake_prepare(root, names, pool=128, on_status=None):
    steps = [
        ("Scanning cells  40 / 186   ·   12 servers", 0.05),
        ("Scanning cells  186 / 186   ·   25 servers", 0.25),
        ("Looking up regional caches  1200 / 2400", 0.43),
        ("Checking which caches serve this depot  10 / 40", 0.70),
        ("Ready", 1.0),
    ]
    for msg, frac in steps:
        if on_status:
            on_status(msg, frac)
        time.sleep(0.12)
    raise RuntimeError("stop-for-test")

tester.prepare_catalog = fake_prepare
tester.steam_root = lambda: "x"
tester.load_cell_names = lambda *a, **k: {}

orig = tk.Tk.mainloop

def mainloop(self, n=0):
    def sample():
        texts = []
        def walk(w):
            if w.winfo_class() == "Label":
                texts.append(w.cget("text"))
            for c in w.winfo_children():
                walk(c)
        for child in self.winfo_children():
            walk(child)
        # toplevels are not children; walk every window
        for name in self.tk.splitlist(self.tk.call("winfo", "children", ".")):
            walk(self.nametowidget(name))
        seen["texts"] = texts
        pcts = [t for t in texts if t.endswith("%") and t[:-1].isdigit()]
        seen["pct"] = pcts
        self.destroy()
    self.after(700, sample)
    return orig(self, n)

tk.Tk.mainloop = mainloop
tester.launch_gui()
print("texts", seen.get("texts"))
print("pct", seen.get("pct"))
pct = seen.get("pct") or ["0%"]
value = int(pct[-1].rstrip("%"))
if value < 15:
    raise SystemExit(f"bar did not advance: {pct}")
print("ok", value)
