import customtkinter as ctk

class SlidePanel(ctk.CTkFrame):
	
	global button
	def __init__(self, parent, start_pos, end_pos):
		super().__init__(master = parent)
		
		# general attributes 
		self.start_pos = start_pos + 0.04
		self.end_pos = end_pos - 0.03
		self.width = abs(start_pos - end_pos)

		# animation logic
		self.pos = self.start_pos
		self.in_start_pos = True
		# layout
		self.place(relx = self.start_pos, rely = 0.05, relwidth = self.width, relheight = 0.9)

	def animate(self):
		if self.in_start_pos:
			self.animate_forward()
		else:
			self.animate_backwards()

	def animate_forward(self):
		if self.pos > self.end_pos:
			self.pos -= 0.008
			button.configure(text='Back')
			self.place(relx = self.pos, rely = 0.05, relwidth = self.width, relheight = 0.9)
			self.after(10, self.animate_forward)
		else:
			self.in_start_pos = False

	def animate_backwards(self):
		if self.pos < self.start_pos:
			self.pos += 0.008
			button.configure(text='Get Started')
			self.place(relx = self.pos, rely = 0.05, relwidth = self.width, relheight = 0.9)
			self.after(10, self.animate_backwards)
		else:
			self.in_start_pos = True

# window 
window = ctk.CTk()
window_width = 840
window_height = 550

ctk.set_appearance_mode('light')
ctk.set_default_color_theme('green')
screen_width = window.winfo_screenwidth()
screen_height = window.winfo_screenheight()

x = (screen_width // 2) - (window_width // 2)
y = (screen_height // 2) - (window_height // 2)

window.geometry(f"{window_width}x{window_height}+{x}+{y}")
window.resizable(False, False)

# animated widget
animated_panel = SlidePanel(window, 1.0, 0.75)

ctk.CTkButton(animated_panel, text = ' Load', fg_color=("#DB3E39", "#821D1A"), corner_radius = 0).place(relx = 0.5, rely = 0.3, anchor = 'center')
ctk.CTkButton(animated_panel, text = ' Export', fg_color=("#DB3E39", "#821D1A"), corner_radius = 0).place(relx = 0.5, rely = 0.4, anchor = 'center')
ctk.CTkButton(animated_panel, text = ' Databases', fg_color=("#DB3E39", "#821D1A"), corner_radius = 0).place(relx = 0.5, rely = 0.5, anchor = 'center')

button = ctk.CTkButton(window, text = 'Get Started', command = animated_panel.animate)
button.place(relx = 0.5, rely = 0.95, anchor = 'center')


# run
window.mainloop()
