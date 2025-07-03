import os
import re
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path
from collections import defaultdict
from PIL import Image, ImageTk

try:
    import pytesseract
except ImportError:
    print("Pytesseract not found. Please install it with 'pip install pytesseract'")
    exit()

# --- CONFIGURATION ---
TESSERACT_CMD_PATH = r'' # Example for Windows: r'C:\Program Files\Tesseract-OCR\tesseract.exe'
ROOT_FOLDER = r'/home/taco/projects/recodeWaterfall/images' # Example: r'C:\Users\YourUser\Desktop\MyImages'
# --- END OF CONFIGURATION ---

if TESSERACT_CMD_PATH:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD_PATH

# Crop dimensions
ORIGINAL_WIDTH, ORIGINAL_HEIGHT = 1920, 1080
MAIN_CROP_BOX = (510, 38, ORIGINAL_WIDTH - 29, ORIGINAL_HEIGHT - 124)
OCR_CROP_BOX = (550, 959, ORIGINAL_WIDTH - 855, ORIGINAL_HEIGHT - 99)


class CropValidatorApp:
    def __init__(self, root, image_paths):
        self.root = root
        self.image_paths = image_paths
        self.current_index = 0
        self.name_counter = defaultdict(int)

        self.root.title("Image Crop Validator")
        self.root.geometry("1200x600")

        # --- Widgets ---
        self.progress_label = ttk.Label(root, text="", font=("Helvetica", 12))
        self.progress_label.pack(pady=5)

        # Image frames
        image_frame = ttk.Frame(root)
        image_frame.pack(pady=10, padx=10, expand=True, fill='both')
        
        self.original_label = ttk.Label(image_frame, text="Original")
        self.original_label.pack(side='left', anchor='n', padx=10)
        self.original_img_label = ttk.Label(image_frame)
        self.original_img_label.pack(side='left', expand=True)

        self.cropped_label = ttk.Label(image_frame, text="Cropped Preview")
        self.cropped_label.pack(side='left', anchor='n', padx=10)
        self.cropped_img_label = ttk.Label(image_frame)
        self.cropped_img_label.pack(side='left', expand=True)

        # Form fields
        form_frame = ttk.Frame(root)
        form_frame.pack(pady=10)
        
        ttk.Label(form_frame, text="Folder Name:").grid(row=0, column=0, padx=5, pady=5)
        self.folder_var = tk.StringVar()
        self.folder_entry = ttk.Entry(form_frame, textvariable=self.folder_var, width=30)
        self.folder_entry.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(form_frame, text="Script Name:").grid(row=1, column=0, padx=5, pady=5)
        self.script_var = tk.StringVar()
        self.script_entry = ttk.Entry(form_frame, textvariable=self.script_var, width=30)
        self.script_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(form_frame, text="Increment:").grid(row=2, column=0, padx=5, pady=5)
        self.inc_var = tk.StringVar()
        self.inc_entry = ttk.Entry(form_frame, textvariable=self.inc_var, width=10)
        self.inc_entry.grid(row=2, column=1, padx=5, pady=5, sticky='w')

        # Buttons
        button_frame = ttk.Frame(root)
        button_frame.pack(pady=10)
        
        self.confirm_button = ttk.Button(button_frame, text="Confirm & Save", command=self.confirm_and_save)
        self.confirm_button.pack(side='left', padx=10)
        
        self.skip_button = ttk.Button(button_frame, text="Skip", command=self.load_next_image)
        self.skip_button.pack(side='left', padx=10)

        # Load the first image
        self.load_next_image()

    def load_next_image(self):
        if self.current_index >= len(self.image_paths):
            messagebox.showinfo("Done", "All images have been processed!")
            self.root.quit()
            return

        image_path = self.image_paths[self.current_index]
        self.current_index += 1
        
        self.progress_label.config(text=f"Processing Image {self.current_index} of {len(self.image_paths)}")

        try:
            # Open images
            self.original_pil_img = Image.open(image_path)
            self.cropped_pil_img = self.original_pil_img.crop(MAIN_CROP_BOX)
            
            # Create display-sized versions
            original_display = self.original_pil_img.copy()
            original_display.thumbnail((550, 450))
            self.original_tk_img = ImageTk.PhotoImage(original_display)
            self.original_img_label.config(image=self.original_tk_img)

            cropped_display = self.cropped_pil_img.copy()
            cropped_display.thumbnail((550, 450))
            self.cropped_tk_img = ImageTk.PhotoImage(cropped_display)
            self.cropped_img_label.config(image=self.cropped_tk_img)
            
            # Pre-fill data
            folder_name = image_path.parent.name
            
            ocr_crop_img = self.original_pil_img.crop(OCR_CROP_BOX)
            ocr_text = pytesseract.image_to_string(ocr_crop_img, config='--oem 3 --psm 7').strip()

            if '/' in ocr_text:
                script_name = ocr_text.split('/')[-1]
                script_name = re.sub(r'[\\/*?:"<>|]', "", script_name)
            else:
                script_name = "UNKNOWN_SCRIPT" # Default if OCR fails
            
            # Determine prospective increment
            prospective_key = (folder_name, script_name)
            increment_num = self.name_counter[prospective_key] + 1

            self.folder_var.set(folder_name)
            self.script_var.set(script_name)
            self.inc_var.set(str(increment_num))

        except Exception as e:
            messagebox.showerror("Error", f"Could not process {image_path.name}:\n{e}")
            self.load_next_image() # Skip to next on error

    def confirm_and_save(self):
        folder_name = self.folder_var.get()
        script_name = self.script_var.get()
        increment_num = self.inc_var.get()

        if not all([folder_name, script_name, increment_num]):
            messagebox.showwarning("Warning", "All fields must be filled.")
            return

        try:
            # Use confirmed values to save file and update counter
            name_key = (folder_name, script_name)
            self.name_counter[name_key] += 1
            
            new_filename = f"{folder_name} {script_name} {increment_num}.png"
            image_path = self.image_paths[self.current_index - 1]
            output_path = image_path.parent / new_filename
            
            # Save the full-resolution cropped image
            self.cropped_pil_img.save(output_path)
            
        except Exception as e:
            messagebox.showerror("Save Error", f"Could not save the file:\n{e}")
            # Decrement counter on failure to allow retry
            self.name_counter[name_key] -= 1
            return
            
        self.load_next_image()

def find_screenshot_files(root_dir):
    root_path = Path(root_dir)
    if not root_path.is_dir():
        return []
    all_files = list(root_path.rglob('*Screenshot*.png'))
    def get_sort_key(file_path):
        match = re.search(r'(\d{4}-\d{2}-\d{2}\s\d{2}-\d{2}-\d{2})', str(file_path.name))
        return match.group(1) if match else ""
    return sorted(all_files, key=get_sort_key)

if __name__ == '__main__':
    if ROOT_FOLDER == '':
        print("!!! PLEASE SET THE 'ROOT_FOLDER' VARIABLE IN THE SCRIPT FIRST !!!")
    else:
        image_files = find_screenshot_files(ROOT_FOLDER)
        if not image_files:
            print("No screenshot files found to process.")
        else:
            root = tk.Tk()
            app = CropValidatorApp(root, image_files)
            root.mainloop()
