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

ctk.CTkButton(animated_panel, text = 'Genesys Load', fg_color=("#DB3E39", "#821D1A"), corner_radius = 0).place(relx = 0.5, rely = 0.3, anchor = 'center')
ctk.CTkButton(animated_panel, text = 'Genesys Export', fg_color=("#DB3E39", "#821D1A"), corner_radius = 0).place(relx = 0.5, rely = 0.4, anchor = 'center')
ctk.CTkButton(animated_panel, text = 'i3 Databases', fg_color=("#DB3E39", "#821D1A"), corner_radius = 0).place(relx = 0.5, rely = 0.5, anchor = 'center')

button = ctk.CTkButton(window, text = 'Get Started', command = animated_panel.animate)
button.place(relx = 0.5, rely = 0.95, anchor = 'center')

Note = ctk.CTkTextbox(window, fg_color = 'transparent', width=530, height=450, corner_radius= 5, border_color='darkgray', border_width=3, font=("Times New Roman Greek", 14), wrap=ctk.WORD,text_color="black")
Note.insert('end', '''Genesys Load ::\nTo truncate the Genesys Contact List and re load the i3 db data into the Genesys Contact List.\n\n                                                            OR\nReload the partial data without truncating the Genesys Contact List.
                  \n\nGenesys Export ::\nThis will re export the data from the Genesys Contact List to the FTP location.\n\n                                                            OR\nIf the csv File is already exported then to insert the data into Raw table.
                  \n\ni3 Databases ::\nIf the ask is to Truncate the Genesys Contact List and re load the i3 db data into the Genesys Contact List.\nThen you have to update the value of CqNotes as'Loaded' in i3 Database Callqueue Table.\n''')
Note.configure(state="disabled")
Note.place(relx = 0.35, rely = 0.46, anchor = 'center')
Note.focus_set()
# run
window.mainloop()
