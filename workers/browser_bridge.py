#!/usr/bin/env python3
"""Playwright bridge — reads JSON actions from stdin, writes results to stdout."""
import sys
import json

def run_action(action: dict) -> dict:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"success": False, "output": "playwright not installed. Run: pip install playwright && playwright install chromium"}
    
    op = action.get("operation", "")
    url = action.get("url", "")
    headless = action.get("headless", True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        try:
            if op == "navigate_and_extract":
                page.goto(url, timeout=30000)
                text = page.inner_text("body")
                return {"success": True, "output": text[:5000]}
            
            elif op == "screenshot":
                page.goto(url, timeout=30000)
                path = action.get("path", "screenshot.png")
                page.screenshot(path=path)
                return {"success": True, "output": f"screenshot saved to {path}"}
            
            elif op == "click":
                page.goto(url, timeout=30000)
                page.click(action.get("selector", ""))
                text = page.inner_text("body")
                return {"success": True, "output": text[:3000]}
            
            elif op == "fill_form":
                page.goto(url, timeout=30000)
                for selector, value in action.get("fields", {}).items():
                    page.fill(selector, value)
                if submit := action.get("submit"):
                    page.click(submit)
                text = page.inner_text("body")
                return {"success": True, "output": text[:3000]}
            
            else:
                return {"success": False, "output": f"unknown operation: {op}. Use: navigate_and_extract, screenshot, click, fill_form"}
        
        except Exception as e:
            return {"success": False, "output": str(e)}
        finally:
            browser.close()

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            action = json.loads(line)
            result = run_action(action)
        except Exception as e:
            result = {"success": False, "output": str(e)}
        print(json.dumps(result), flush=True)
