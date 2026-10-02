import time
import tkinter as tk
import tester

seen = {"grab": False, "open_texts": [], "closed": None, "status": None}

class Track(tk.Toplevel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        seen["win"] = self

    def grab_set(self):
        seen["grab"] = True
        return super().grab_set()

tk.Toplevel = Track

def fake_prepare(root, names, pool=128, on_status=None):
    time.sleep(0.35)
    if on_status:
        on_status("Scanning cells 20/186  ·  9 PoPs")
    time.sleep(0.45)
    raise RuntimeError("stop-for-test")

tester.prepare_catalog = fake_prepare
tester.steam_root = lambda: "x"
tester.load_cell_names = lambda *a, **k: {}

orig = tk.Tk.mainloop

def mainloop(self, n=0):
    def check_open():
        win = seen.get("win")
        seen["open_alive"] = bool(win and win.winfo_exists())
        labels = []
        if win and win.winfo_exists():
            def walk(w):
                if w.winfo_class() == "Label":
                    labels.append(w.cget("text"))
                for c in w.winfo_children():
                    walk(c)
            walk(win)
        seen["open_texts"] = labels
        seen["grab_current"] = str(self.grab_current())
    def check_closed():
        win = seen.get("win")
        seen["closed"] = not (win and win.winfo_exists())
        # status line on the main window
        def walk(w, out):
            if w.winfo_class() == "Label":
                out.append(w.cget("text"))
            for c in w.winfo_children():
                walk(c, out)
        texts = []
        walk(self, texts)
        seen["status"] = texts
        self.destroy()
    self.after(500, check_open)
    self.after(1100, check_closed)
    return orig(self, n)

tk.Tk.mainloop = mainloop
tester.launch_gui()
print("grab", seen["grab"])
print("open_alive", seen.get("open_alive"))
print("grab_current", seen.get("grab_current"))
print("open_texts", seen.get("open_texts"))
print("closed", seen.get("closed"))
print("status", seen.get("status"))
if not seen["grab"] or not seen.get("open_alive"):
    raise SystemExit("modal was not showing during load")
if "Scanning cells 20/186  ·  9 PoPs" not in seen.get("open_texts", []):
    raise SystemExit("modal did not show scan progress")
if not seen.get("closed"):
    raise SystemExit("modal stayed up after load finished")
print("ok")
