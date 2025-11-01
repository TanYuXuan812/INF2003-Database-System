import tkinter as tk
from tkinter import ttk, messagebox
# from tkinter.simpledialog import askstring, askinteger
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

# Import admin functions from admin_query.py
from admin_query import (
    admin_create_movie,
    admin_get_movie,
    admin_search_movies_by_title,
    admin_update_movie,
    admin_delete_movie,
    admin_create_genre,
    # admin_get_genre,
    admin_search_genres_by_name,
    admin_read_genres,
    admin_update_genre,
    admin_delete_genre,
    admin_create_company,
    # admin_get_company,
    admin_search_companies_by_name,
    admin_read_companies,
    admin_update_company,
    admin_delete_company,
    admin_get_company_with_movies,
    # admin_get_companies_with_stats,
    admin_get_rating,
    admin_read_ratings,
    admin_update_rating,
    admin_delete_rating,
    admin_create_movie_genre,
    admin_get_movie_genre,
    admin_read_movie_genres,
    admin_delete_movie_genre,
    admin_create_movie_company,
    admin_get_movie_company,
    admin_read_movie_companies,
    admin_delete_movie_company,
    validate_gender,
    get_gender_display,
    admin_create_person,
    admin_get_person,
    admin_search_people_by_name,
    admin_read_people,
    admin_update_person,
    admin_delete_person,
    admin_create_movie_cast,
    admin_get_movie_cast,
    admin_read_movie_casts,
    admin_update_movie_cast,
    admin_delete_movie_cast,
    admin_create_movie_crew,
    admin_get_movie_crew,
    admin_read_movie_crews,
    admin_update_movie_crew,
    admin_delete_movie_crew
    )

load_dotenv()

# ----------------------------------------------------
# DB connection helper
# ----------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DATABASE"),
        user=os.getenv("DB_USER"),
        password=os.getenv("PASSWORD"),
        host=os.getenv("HOST"),
        port=os.getenv("PORT")
    )

# ----------------------------------------------------
# Helper: safe cursor context manager
# ----------------------------------------------------
class DbCtx:
    def __enter__(self):
        self.conn = get_db_connection()
        self.cur = self.conn.cursor()
        return self.cur, self.conn

    def __exit__(self, exc_type, exc, tb):
        if exc:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.cur.close()
        self.conn.close()

# ----------------------------------------------------
# Main Application
# ----------------------------------------------------

class MovieAdminApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Movie Database Admin")
        self.root.geometry("1200x700")
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Create tabs
        self.movies_tab = ttk.Frame(self.notebook)
        self.genres_tab = ttk.Frame(self.notebook)
        self.companies_tab = ttk.Frame(self.notebook)
        self.ratings_tab = ttk.Frame(self.notebook)
        self.movie_genres_tab = ttk.Frame(self.notebook)
        self.movie_companies_tab = ttk.Frame(self.notebook)
        self.people_tab = ttk.Frame(self.notebook)
        self.movie_cast_tab = ttk.Frame(self.notebook)
        self.movie_crew_tab = ttk.Frame(self.notebook)

        self.notebook.add(self.movies_tab, text='Movies')
        self.notebook.add(self.genres_tab, text='Genres')
        self.notebook.add(self.companies_tab, text='P.Companies')
        self.notebook.add(self.ratings_tab, text='Ratings')
        self.notebook.add(self.movie_genres_tab, text="Add Movie-Genres") 
        self.notebook.add(self.movie_companies_tab, text="Add Movie-Companies")
        self.notebook.add(self.people_tab, text="People")
        self.notebook.add(self.movie_cast_tab, text="Movie Cast")
        self.notebook.add(self.movie_crew_tab, text="Movie Crew")
        
        # Setup tabs
        self.setup_movies_tab()
        self.setup_genres_tab()
        self.setup_companies_tab()
        self.setup_ratings_tab()
        self.setup_movie_genres_tab()
        self.setup_movie_companies_tab()
        self.setup_people_tab()
        self.setup_movie_cast_tab()
        self.setup_movie_crew_tab()  # Add this line

    # ============================================
    # MOVIES TAB
    # ============================================
    
    def setup_movies_tab(self):
        # Search frame
        search_frame = ttk.LabelFrame(self.movies_tab, text="Search Movies", padding=10)
        search_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(search_frame, text="Title:").grid(row=0, column=0, padx=5)
        self.movie_search_entry = ttk.Entry(search_frame, width=40)
        self.movie_search_entry.grid(row=0, column=1, padx=5)
        self.movie_search_entry.bind('<Return>', lambda e: self.search_movies())
        
        ttk.Button(search_frame, text="Search", command=self.search_movies).grid(row=0, column=2, padx=5)
        ttk.Button(search_frame, text="Clear", command=self.clear_movie_search).grid(row=0, column=3, padx=5)
        ttk.Button(search_frame, text="Create New Movie", command=self.create_movie_dialog).grid(row=0, column=4, padx=5)
        
        # Results frame with treeview
        results_frame = ttk.LabelFrame(self.movies_tab, text="Search Results", padding=10)
        results_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Treeview with scrollbar
        tree_scroll = ttk.Scrollbar(results_frame)
        tree_scroll.pack(side='right', fill='y')
        
        self.movies_tree = ttk.Treeview(results_frame, yscrollcommand=tree_scroll.set, 
                                        columns=('ID', 'Title', 'Released'), show='headings')
        self.movies_tree.heading('ID', text='ID')
        self.movies_tree.heading('Title', text='Title')
        self.movies_tree.heading('Released', text='Release Date')
        
        self.movies_tree.column('ID', width=80)
        self.movies_tree.column('Title', width=400)
        self.movies_tree.column('Released', width=120)
        
        self.movies_tree.pack(fill='both', expand=True)
        tree_scroll.config(command=self.movies_tree.yview)
        
        # Bind double-click to view details
        self.movies_tree.bind('<Double-Button-1>', lambda e: self.view_movie_details())
        
        # Action buttons
        action_frame = ttk.Frame(self.movies_tab)
        action_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(action_frame, text="View Details", command=self.view_movie_details).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Edit Movie", command=self.edit_movie_dialog).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Delete Movie", command=self.delete_movie).pack(side='left', padx=5)
    
    def search_movies(self):
        search_term = self.movie_search_entry.get().strip()
        if not search_term:
            messagebox.showwarning("Warning", "Please enter a search term")
            return
        
        try:
            with DbCtx() as (cur, conn):
                results = admin_search_movies_by_title(cur, search_term)
            
            # Clear existing items
            for item in self.movies_tree.get_children():
                self.movies_tree.delete(item)
            
            # Add results
            for movie in results:
                self.movies_tree.insert('', 'end', values=(
                    movie['movie_id'],
                    movie['title'],
                    movie['released_date'] if movie['released_date'] else 'N/A'
                ))
            
            if not results:
                messagebox.showinfo("No Results", f"No movies found matching '{search_term}'")
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def clear_movie_search(self):
        self.movie_search_entry.delete(0, tk.END)
        for item in self.movies_tree.get_children():
            self.movies_tree.delete(item)
    
    def view_movie_details(self):
        selection = self.movies_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a movie")
            return
        
        movie_id = self.movies_tree.item(selection[0])['values'][0]
        
        try:
            with DbCtx() as (cur, conn):
                movie = admin_get_movie(cur, movie_id)
            
            if not movie:
                messagebox.showerror("Error", "Movie not found")
                return
            
            # Create details window
            details_win = tk.Toplevel(self.root)
            details_win.title(f"Movie Details - {movie['title']}")
            details_win.geometry("600x500")
            
            # Display details
            frame = ttk.Frame(details_win, padding=20)
            frame.pack(fill='both', expand=True)
            
            fields = [
                ('ID:', movie['movie_id']),
                ('Title:', movie['title']),
                ('Adult:', 'Yes' if movie['adult'] else 'No'),
                ('Language:', movie['language'] or 'N/A'),
                ('Popularity:', movie['popularity'] or 'N/A'),
                ('Released:', movie['released_date'] or 'N/A'),
                ('Runtime:', f"{movie['runtime']} min" if movie['runtime'] else 'N/A'),
                ('Poster Path:', movie['poster_path'] or 'N/A'),
                ('Tagline:', movie['tagline'] or 'N/A'),
                ('Overview:', movie['overview'] or 'N/A'),
            ]
            
            for i, (label, value) in enumerate(fields):
                ttk.Label(frame, text=label, font=('Arial', 10, 'bold')).grid(row=i, column=0, sticky='nw', padx=5, pady=5)
                if label == 'Overview:':
                    text_widget = tk.Text(frame, height=5, width=50, wrap='word')
                    text_widget.insert('1.0', str(value))
                    text_widget.config(state='disabled')
                    text_widget.grid(row=i, column=1, sticky='w', padx=5, pady=5)
                else:
                    ttk.Label(frame, text=str(value)).grid(row=i, column=1, sticky='w', padx=5, pady=5)
            
            ttk.Button(details_win, text="Close", command=details_win.destroy).pack(pady=10)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load details: {str(e)}")
    
    def create_movie_dialog(self):
        dialog = tk.Toplevel(self.root)
        dialog.title("Create New Movie")
        dialog.geometry("500x600")
        
        frame = ttk.Frame(dialog, padding=20)
        frame.pack(fill='both', expand=True)
        
        # Entry fields
        entries = {}
        fields = [
            ('movie_id', 'Movie ID*:', 'int'),
            ('title', 'Title*:', 'str'),
            ('adult', 'Adult (0/1):', 'bool'),
            ('language', 'Language:', 'str'),
            ('popularity', 'Popularity:', 'float'),
            ('released_date', 'Released (YYYY-MM-DD):', 'date'),
            ('runtime', 'Runtime (minutes):', 'int'),
            ('poster_path', 'Poster Path:', 'str'),
            ('tagline', 'Tagline:', 'str'),
        ]
        
        for i, (key, label, dtype) in enumerate(fields):
            ttk.Label(frame, text=label).grid(row=i, column=0, sticky='w', padx=5, pady=5)
            entry = ttk.Entry(frame, width=40)
            entry.grid(row=i, column=1, padx=5, pady=5)
            entries[key] = (entry, dtype)
        
        # Overview field (text widget)
        ttk.Label(frame, text='Overview:').grid(row=len(fields), column=0, sticky='nw', padx=5, pady=5)
        overview_text = tk.Text(frame, height=5, width=40, wrap='word')
        overview_text.grid(row=len(fields), column=1, padx=5, pady=5)
        
        def save_movie():
            try:
                movie_data = {}
                for key, (entry, dtype) in entries.items():
                    value = entry.get().strip()
                    if not value:
                        if key in ['movie_id', 'title']:
                            messagebox.showerror("Error", f"{key} is required")
                            return
                        movie_data[key] = None
                    else:
                        if dtype == 'int':
                            movie_data[key] = int(value)
                        elif dtype == 'float':
                            movie_data[key] = float(value)
                        elif dtype == 'bool':
                            movie_data[key] = bool(int(value))
                        elif dtype == 'date':
                            movie_data[key] = datetime.strptime(value, '%Y-%m-%d').date()
                        else:
                            movie_data[key] = value
                
                overview = overview_text.get('1.0', 'end-1c').strip()
                movie_data['overview'] = overview if overview else None
                
                with DbCtx() as (cur, conn):
                    admin_create_movie(cur, movie_data)
                
                messagebox.showinfo("Success", "Movie created successfully")
                dialog.destroy()
                self.search_movies()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to create movie: {str(e)}")
        
        ttk.Button(frame, text="Save", command=save_movie).grid(row=len(fields)+1, column=0, pady=20)
        ttk.Button(frame, text="Cancel", command=dialog.destroy).grid(row=len(fields)+1, column=1, pady=20)
    
    def edit_movie_dialog(self):
        selection = self.movies_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a movie to edit")
            return
        
        movie_id = self.movies_tree.item(selection[0])['values'][0]
        
        try:
            with DbCtx() as (cur, conn):
                movie = admin_get_movie(cur, movie_id)
            
            if not movie:
                messagebox.showerror("Error", "Movie not found")
                return
            
            dialog = tk.Toplevel(self.root)
            dialog.title(f"Edit Movie - {movie['title']}")
            dialog.geometry("500x600")
            
            frame = ttk.Frame(dialog, padding=20)
            frame.pack(fill='both', expand=True)
            
            entries = {}
            fields = [
                ('title', 'Title:', 'str', movie['title']),
                ('adult', 'Adult (0/1):', 'bool', '1' if movie['adult'] else '0'),
                ('language', 'Language:', 'str', movie['language']),
                ('popularity', 'Popularity:', 'float', movie['popularity']),
                ('released_date', 'Released (YYYY-MM-DD):', 'date', movie['released_date']),
                ('runtime', 'Runtime:', 'int', movie['runtime']),
                ('poster_path', 'Poster Path:', 'str', movie['poster_path']),
                ('tagline', 'Tagline:', 'str', movie['tagline']),
            ]
            
            for i, (key, label, dtype, value) in enumerate(fields):
                ttk.Label(frame, text=label).grid(row=i, column=0, sticky='w', padx=5, pady=5)
                entry = ttk.Entry(frame, width=40)
                if value:
                    entry.insert(0, str(value))
                entry.grid(row=i, column=1, padx=5, pady=5)
                entries[key] = (entry, dtype)
            
            ttk.Label(frame, text='Overview:').grid(row=len(fields), column=0, sticky='nw', padx=5, pady=5)
            overview_text = tk.Text(frame, height=5, width=40, wrap='word')
            if movie['overview']:
                overview_text.insert('1.0', movie['overview'])
            overview_text.grid(row=len(fields), column=1, padx=5, pady=5)
            
            def update_movie():
                try:
                    update_data = {}
                    for key, (entry, dtype) in entries.items():
                        value = entry.get().strip()
                        if value:
                            if dtype == 'int':
                                update_data[key] = int(value)
                            elif dtype == 'float':
                                update_data[key] = float(value)
                            elif dtype == 'bool':
                                update_data[key] = bool(int(value))
                            elif dtype == 'date':
                                update_data[key] = datetime.strptime(value, '%Y-%m-%d').date()
                            else:
                                update_data[key] = value
                    
                    overview = overview_text.get('1.0', 'end-1c').strip()
                    if overview:
                        update_data['overview'] = overview
                    
                    with DbCtx() as (cur, conn):
                        admin_update_movie(cur, movie_id, update_data)
                    
                    messagebox.showinfo("Success", "Movie updated successfully")
                    dialog.destroy()
                    self.search_movies()
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to update movie: {str(e)}")
            
            ttk.Button(frame, text="Update", command=update_movie).grid(row=len(fields)+1, column=0, pady=20)
            ttk.Button(frame, text="Cancel", command=dialog.destroy).grid(row=len(fields)+1, column=1, pady=20)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load movie: {str(e)}")
    
    def delete_movie(self):
        selection = self.movies_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a movie to delete")
            return
        
        movie_id = self.movies_tree.item(selection[0])['values'][0]
        movie_title = self.movies_tree.item(selection[0])['values'][1]
        
        if not messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete '{movie_title}'?\n\nThis will cascade to related tables."):
            return
        
        try:
            with DbCtx() as (cur, conn):
                admin_delete_movie(cur, movie_id)
            
            messagebox.showinfo("Success", "Movie deleted successfully")
            self.search_movies()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete movie: {str(e)}")
    
    # ============================================
    # GENRES TAB
    # ============================================
    
    def setup_genres_tab(self):
        # Top frame with buttons
        top_frame = ttk.Frame(self.genres_tab)
        top_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(top_frame, text="Load All Genres", command=self.load_all_genres).pack(side='left', padx=5)
        ttk.Button(top_frame, text="Create New Genre", command=self.create_genre_dialog).pack(side='left', padx=5)
        
        # Search frame
        search_frame = ttk.LabelFrame(self.genres_tab, text="Search Genres", padding=10)
        search_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(search_frame, text="Genre Name:").grid(row=0, column=0, padx=5)
        self.genre_search_entry = ttk.Entry(search_frame, width=40)
        self.genre_search_entry.grid(row=0, column=1, padx=5)
        self.genre_search_entry.bind('<Return>', lambda e: self.search_genres())
        
        ttk.Button(search_frame, text="Search", command=self.search_genres).grid(row=0, column=2, padx=5)
        ttk.Button(search_frame, text="Clear", command=self.clear_genre_search).grid(row=0, column=3, padx=5)
        
        # Results frame with treeview
        results_frame = ttk.LabelFrame(self.genres_tab, text="Genres", padding=10)
        results_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Treeview with scrollbar
        tree_scroll = ttk.Scrollbar(results_frame)
        tree_scroll.pack(side='right', fill='y')
        
        self.genres_tree = ttk.Treeview(results_frame, yscrollcommand=tree_scroll.set, 
                                        columns=('ID', 'Genre Name'), show='headings')
        self.genres_tree.heading('ID', text='ID')
        self.genres_tree.heading('Genre Name', text='Genre Name')
        
        self.genres_tree.column('ID', width=100)
        self.genres_tree.column('Genre Name', width=300)
        
        self.genres_tree.pack(fill='both', expand=True)
        tree_scroll.config(command=self.genres_tree.yview)
        
        # Action buttons
        action_frame = ttk.Frame(self.genres_tab)
        action_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(action_frame, text="Edit Genre", command=self.edit_genre_dialog).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Delete Genre", command=self.delete_genre).pack(side='left', padx=5)
    
    def load_all_genres(self):
        try:
            with DbCtx() as (cur, conn):
                genres = admin_read_genres(cur)
            
            # Clear existing items
            for item in self.genres_tree.get_children():
                self.genres_tree.delete(item)
            
            # Add genres
            for genre in genres:
                self.genres_tree.insert('', 'end', values=(
                    genre['genre_id'],
                    genre['genre_name']
                ))
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load genres: {str(e)}")
    
    def search_genres(self):
        search_term = self.genre_search_entry.get().strip()
        if not search_term:
            messagebox.showwarning("Warning", "Please enter a search term")
            return
        
        try:
            with DbCtx() as (cur, conn):
                results = admin_search_genres_by_name(cur, search_term)
            
            # Clear existing items
            for item in self.genres_tree.get_children():
                self.genres_tree.delete(item)
            
            # Add results
            for genre in results:
                self.genres_tree.insert('', 'end', values=(
                    genre['genre_id'],
                    genre['genre_name']
                ))
            
            if not results:
                messagebox.showinfo("No Results", f"No genres found matching '{search_term}'")
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def clear_genre_search(self):
        self.genre_search_entry.delete(0, tk.END)
        self.load_all_genres()
    
    def create_genre_dialog(self):
        genre_name = tk.simpledialog.askstring("Create Genre", "Enter genre name:")
        if not genre_name:
            return
        
        try:
            with DbCtx() as (cur, conn):
                result = admin_create_genre(cur, genre_name)
            
            if result:
                messagebox.showinfo("Success", f"Genre '{genre_name}' created successfully")
                self.load_all_genres()
            else:
                messagebox.showwarning("Warning", f"Genre '{genre_name}' already exists")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to create genre: {str(e)}")
    
    def edit_genre_dialog(self):
        selection = self.genres_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a genre to edit")
            return
        
        genre_id = self.genres_tree.item(selection[0])['values'][0]
        current_name = self.genres_tree.item(selection[0])['values'][1]
        
        new_name = tk.simpledialog.askstring("Edit Genre", "Enter new genre name:", initialvalue=current_name)
        if not new_name or new_name == current_name:
            return
        
        try:
            with DbCtx() as (cur, conn):
                admin_update_genre(cur, genre_id, new_name)
            
            messagebox.showinfo("Success", "Genre updated successfully")
            self.load_all_genres()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to update genre: {str(e)}")
    
    def delete_genre(self):
        selection = self.genres_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a genre to delete")
            return
        
        genre_id = self.genres_tree.item(selection[0])['values'][0]
        genre_name = self.genres_tree.item(selection[0])['values'][1]
        
        if not messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete '{genre_name}'?"):
            return
        
        try:
            with DbCtx() as (cur, conn):
                admin_delete_genre(cur, genre_id)
            
            messagebox.showinfo("Success", "Genre deleted successfully")
            self.load_all_genres()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete genre: {str(e)}")

    # ============================================
    # PRODUCTION COMPANIES TAB
    # ============================================
    def setup_companies_tab(self):
        # Top frame with buttons
        top_frame = ttk.Frame(self.companies_tab)
        top_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(top_frame, text="Load All Companies", command=self.load_all_companies).pack(side='left', padx=5)
        ttk.Button(top_frame, text="Create New Company", command=self.create_company_dialog).pack(side='left', padx=5)
        
        # Search frame
        search_frame = ttk.LabelFrame(self.companies_tab, text="Search Companies", padding=10)
        search_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(search_frame, text="Company Name:").grid(row=0, column=0, padx=5)
        self.company_search_entry = ttk.Entry(search_frame, width=40)
        self.company_search_entry.grid(row=0, column=1, padx=5)
        self.company_search_entry.bind('<Return>', lambda e: self.search_companies())
        
        ttk.Button(search_frame, text="Search", command=self.search_companies).grid(row=0, column=2, padx=5)
        ttk.Button(search_frame, text="Clear", command=self.clear_company_search).grid(row=0, column=3, padx=5)
        
        # Results frame with treeview
        results_frame = ttk.LabelFrame(self.companies_tab, text="Production Companies", padding=10)
        results_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Treeview with scrollbar
        tree_scroll = ttk.Scrollbar(results_frame)
        tree_scroll.pack(side='right', fill='y')
        
        self.companies_tree = ttk.Treeview(results_frame, yscrollcommand=tree_scroll.set, 
                                          columns=('ID', 'Company Name'), show='headings')
        self.companies_tree.heading('ID', text='ID')
        self.companies_tree.heading('Company Name', text='Company Name')
        
        self.companies_tree.column('ID', width=100)
        self.companies_tree.column('Company Name', width=400)
        
        self.companies_tree.pack(fill='both', expand=True)
        tree_scroll.config(command=self.companies_tree.yview)
        
        # Double-click to view details
        self.companies_tree.bind('<Double-1>', lambda e: self.view_company_details())
        
        # Action buttons
        action_frame = ttk.Frame(self.companies_tab)
        action_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(action_frame, text="View Details", command=self.view_company_details).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Edit Company", command=self.edit_company_dialog).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Delete Company", command=self.delete_company).pack(side='left', padx=5)
    
    def load_all_companies(self):
        try:
            with DbCtx() as (cur, conn):
                companies = admin_read_companies(cur)
            
            # Clear existing items
            for item in self.companies_tree.get_children():
                self.companies_tree.delete(item)
            
            # Add companies
            for company in companies:
                self.companies_tree.insert('', 'end', values=(
                    company['company_id'],
                    company['name']
                ))
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load companies: {str(e)}")
    
    def search_companies(self):
        search_term = self.company_search_entry.get().strip()
        if not search_term:
            messagebox.showwarning("Warning", "Please enter a search term")
            return
        
        try:
            with DbCtx() as (cur, conn):
                results = admin_search_companies_by_name(cur, search_term)
            
            # Clear existing items
            for item in self.companies_tree.get_children():
                self.companies_tree.delete(item)
            
            # Add results
            for company in results:
                self.companies_tree.insert('', 'end', values=(
                    company['company_id'],
                    company['name']
                ))
            
            if not results:
                messagebox.showinfo("No Results", f"No companies found matching '{search_term}'")
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def clear_company_search(self):
        self.company_search_entry.delete(0, tk.END)
        self.load_all_companies()
    
    def create_company_dialog(self):
        company_name = tk.simpledialog.askstring("Create Company", "Enter company name:")
        if not company_name:
            return
        
        try:
            with DbCtx() as (cur, conn):
                result = admin_create_company(cur, company_name)
            
            if result:
                messagebox.showinfo("Success", f"Company '{company_name}' created successfully")
                self.load_all_companies()
            else:
                messagebox.showwarning("Warning", f"Company '{company_name}' already exists")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to create company: {str(e)}")
    
    def view_company_details(self):
        selection = self.companies_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a company to view details")
            return
        
        company_id = self.companies_tree.item(selection[0])['values'][0]
        company_name = self.companies_tree.item(selection[0])['values'][1]
        
        try:
            with DbCtx() as (cur, conn):
                # Get company details with movie stats
                company_details = admin_get_company_with_movies(cur, company_id)
            
            if company_details:
                # Create details dialog
                details_window = tk.Toplevel(self.root)
                details_window.title(f"Company Details: {company_name}")
                details_window.geometry("500x300")
                details_window.transient(self.root)
                details_window.grab_set()
                
                # Details frame
                details_frame = ttk.LabelFrame(details_window, text="Company Information", padding=15)
                details_frame.pack(fill='both', expand=True, padx=10, pady=10)
                
                # Company info
                ttk.Label(details_frame, text=f"Company ID: {company_details['company_id']}", 
                         font=('Arial', 10, 'bold')).pack(anchor='w', pady=2)
                ttk.Label(details_frame, text=f"Name: {company_details['name']}", 
                         font=('Arial', 10)).pack(anchor='w', pady=2)
                ttk.Label(details_frame, text=f"Total Movies: {company_details['movie_count']}", 
                         font=('Arial', 10)).pack(anchor='w', pady=2)
                
                # Sample movies
                if company_details['sample_movies']:
                    ttk.Label(details_frame, text="Sample Movies:", 
                             font=('Arial', 10, 'bold')).pack(anchor='w', pady=(10, 5))
                    
                    movies_frame = ttk.Frame(details_frame)
                    movies_frame.pack(fill='x', padx=20)
                    
                    for i, movie in enumerate(company_details['sample_movies'][:8]):  # Show first 8
                        ttk.Label(movies_frame, text=f"• {movie}").grid(row=i//2, column=i%2, 
                                                                       sticky='w', padx=10, pady=1)
                
                # Close button
                ttk.Button(details_frame, text="Close", 
                          command=details_window.destroy).pack(pady=10)
            else:
                messagebox.showerror("Error", "Company not found")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load company details: {str(e)}")
    
    def edit_company_dialog(self):
        selection = self.companies_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a company to edit")
            return
        
        company_id = self.companies_tree.item(selection[0])['values'][0]
        current_name = self.companies_tree.item(selection[0])['values'][1]
        
        new_name = tk.simpledialog.askstring("Edit Company", "Enter new company name:", 
                                           initialvalue=current_name)
        if not new_name or new_name == current_name:
            return
        
        try:
            with DbCtx() as (cur, conn):
                result = admin_update_company(cur, company_id, new_name)
            
            if result:
                messagebox.showinfo("Success", "Company updated successfully")
                self.load_all_companies()
            else:
                messagebox.showerror("Error", "Company not found")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to update company: {str(e)}")
    
    def delete_company(self):
        selection = self.companies_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a company to delete")
            return
        
        company_id = self.companies_tree.item(selection[0])['values'][0]
        company_name = self.companies_tree.item(selection[0])['values'][1]
        
        if not messagebox.askyesno("Confirm Delete", 
                                 f"Are you sure you want to delete '{company_name}'?\n\n"
                                 "This action cannot be undone."):
            return
        
        try:
            with DbCtx() as (cur, conn):
                result = admin_delete_company(cur, company_id)
            
            if result:
                messagebox.showinfo("Success", "Company deleted successfully")
                self.load_all_companies()
            else:
                messagebox.showerror("Error", "Company not found")
        except Exception as e:
            error_msg = str(e)
            if "associated with" in error_msg:
                messagebox.showerror("Cannot Delete", 
                                   f"Cannot delete company: {error_msg}\n\n"
                                   "Please remove all movie associations first.")
            else:
                messagebox.showerror("Error", f"Failed to delete company: {error_msg}")

    #============================================
    # RATINGS TAB
    #============================================
    def setup_ratings_tab(self):
        # Top frame with buttons
        top_frame = ttk.Frame(self.ratings_tab)
        top_frame.pack(fill='x', padx=10, pady=5)
        
        # ttk.Button(top_frame, text="Load All Ratings", command=self.load_all_ratings).pack(side='left', padx=5)
        
        # Search frame
        search_frame = ttk.LabelFrame(self.ratings_tab, text="Search & Filter", padding=10)
        search_frame.pack(fill='x', padx=10, pady=5)
        
        # User ID search
        ttk.Label(search_frame, text="User ID:").grid(row=0, column=0, padx=5, sticky='w')
        self.rating_user_id_entry = ttk.Entry(search_frame, width=15)
        self.rating_user_id_entry.grid(row=0, column=1, padx=5, sticky='w')
        
        # Movie ID search  
        ttk.Label(search_frame, text="Movie ID:").grid(row=0, column=2, padx=5, sticky='w')
        self.rating_movie_id_entry = ttk.Entry(search_frame, width=15)
        self.rating_movie_id_entry.grid(row=0, column=3, padx=5, sticky='w')
        
        # Rating range
        ttk.Label(search_frame, text="Min Rating:").grid(row=1, column=0, padx=5, sticky='w')
        self.min_rating_entry = ttk.Entry(search_frame, width=10)
        self.min_rating_entry.grid(row=1, column=1, padx=5, sticky='w')
        
        ttk.Label(search_frame, text="Max Rating:").grid(row=1, column=2, padx=5, sticky='w')
        self.max_rating_entry = ttk.Entry(search_frame, width=10)
        self.max_rating_entry.grid(row=1, column=3, padx=5, sticky='w')
        
        # Buttons
        ttk.Button(search_frame, text="Search", command=self.search_ratings).grid(row=0, column=4, padx=5)
        ttk.Button(search_frame, text="Clear", command=self.clear_rating_search).grid(row=0, column=5, padx=5)
        
        # Results frame with treeview
        results_frame = ttk.LabelFrame(self.ratings_tab, text="Ratings", padding=10)
        results_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Treeview with scrollbar
        tree_scroll = ttk.Scrollbar(results_frame)
        tree_scroll.pack(side='right', fill='y')
        
        self.ratings_tree = ttk.Treeview(results_frame, yscrollcommand=tree_scroll.set, 
                                        columns=('ID', 'User ID', 'Movie ID', 'Rating', 'Created', 'Updated'), 
                                        show='headings', height=15)
        
        # Configure columns
        self.ratings_tree.heading('ID', text='Rating ID')
        self.ratings_tree.heading('User ID', text='User ID')
        self.ratings_tree.heading('Movie ID', text='Movie ID') 
        self.ratings_tree.heading('Rating', text='Rating')
        self.ratings_tree.heading('Created', text='Created At')
        self.ratings_tree.heading('Updated', text='Updated At')
        
        self.ratings_tree.column('ID', width=80)
        self.ratings_tree.column('User ID', width=80)
        self.ratings_tree.column('Movie ID', width=80)
        self.ratings_tree.column('Rating', width=60)
        self.ratings_tree.column('Created', width=150)
        self.ratings_tree.column('Updated', width=150)
        
        self.ratings_tree.pack(fill='both', expand=True)
        tree_scroll.config(command=self.ratings_tree.yview)
        
        # Double-click to view details
        self.ratings_tree.bind('<Double-1>', lambda e: self.view_rating_details())
        
        # Action buttons
        action_frame = ttk.Frame(self.ratings_tab)
        action_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(action_frame, text="View Details", command=self.view_rating_details).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Edit Rating", command=self.edit_rating_dialog).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Delete Rating", command=self.delete_rating).pack(side='left', padx=5)
        
        # Load initial data
        self.load_all_ratings()
    
    def load_all_ratings(self):
        try:
            with DbCtx() as (cur, conn):
                ratings = admin_read_ratings(cur, limit=200)  # Increased limit for initial load
            
            self.update_ratings_tree(ratings)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load ratings: {str(e)}")
    
    def update_ratings_tree(self, ratings):
        """Update treeview with ratings data"""
        # Clear existing items
        for item in self.ratings_tree.get_children():
            self.ratings_tree.delete(item)
        
        # Add ratings
        for rating in ratings:
            self.ratings_tree.insert('', 'end', values=(
                rating['rating_id'],
                rating['user_id'],
                rating['movie_id'],
                f"{rating['rating']:.1f}" if rating['rating'] is not None else "N/A",
                rating['created_at'].strftime('%Y-%m-%d %H:%M') if rating['created_at'] else "N/A",
                rating['updated_at'].strftime('%Y-%m-%d %H:%M') if rating['updated_at'] else "N/A"
            ))
    
    def search_ratings(self):
        """Search ratings based on criteria"""
        user_id = self.rating_user_id_entry.get().strip()
        movie_id = self.rating_movie_id_entry.get().strip()
        min_rating = self.min_rating_entry.get().strip()
        max_rating = self.max_rating_entry.get().strip()
        
        # If no search criteria, load all
        if not any([user_id, movie_id, min_rating, max_rating]):
            self.load_all_ratings()
            return
        
        try:
            with DbCtx() as (cur, conn):
                # Build dynamic query
                query = """
                SELECT rating_id, user_id, movie_id, rating, created_at, updated_at
                FROM ratings
                WHERE 1=1
                """
                params = []
                
                if user_id:
                    query += " AND user_id = %s"
                    params.append(int(user_id))
                
                if movie_id:
                    query += " AND movie_id = %s"
                    params.append(int(movie_id))
                
                if min_rating:
                    query += " AND rating >= %s"
                    params.append(float(min_rating))
                
                if max_rating:
                    query += " AND rating <= %s"
                    params.append(float(max_rating))
                
                query += " ORDER BY created_at DESC LIMIT 200"
                
                cur.execute(query, params)
                rows = cur.fetchall()
                
                # Convert to dict format
                ratings = [{
                    "rating_id": r[0],
                    "user_id": r[1],
                    "movie_id": r[2],
                    "rating": float(r[3]) if r[3] is not None else None,
                    "created_at": r[4],
                    "updated_at": r[5]
                } for r in rows]
            
            self.update_ratings_tree(ratings)
            
            if not ratings:
                messagebox.showinfo("No Results", "No ratings found matching your criteria")
                
        except ValueError as e:
            messagebox.showerror("Input Error", "Please enter valid numeric values for IDs and ratings")
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def clear_rating_search(self):
        """Clear all search fields and reload all ratings"""
        self.rating_user_id_entry.delete(0, tk.END)
        self.rating_movie_id_entry.delete(0, tk.END)
        self.min_rating_entry.delete(0, tk.END)
        self.max_rating_entry.delete(0, tk.END)
        self.load_all_ratings()
    
    def view_rating_details(self):
        """Show detailed rating information"""
        selection = self.ratings_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a rating to view details")
            return
        
        rating_id = self.ratings_tree.item(selection[0])['values'][0]
        
        try:
            with DbCtx() as (cur, conn):
                rating = admin_get_rating(cur, rating_id)
            
            if rating:
                # Create details dialog
                details_window = tk.Toplevel(self.root)
                details_window.title(f"Rating Details: ID {rating_id}")
                details_window.geometry("500x300")
                details_window.transient(self.root)
                details_window.grab_set()
                
                # Details frame
                details_frame = ttk.LabelFrame(details_window, text="Rating Information", padding=15)
                details_frame.pack(fill='both', expand=True, padx=10, pady=10)
                
                # Rating info
                info_text = f"""Rating ID: {rating['rating_id']}
                User ID: {rating['user_id']}
                Movie ID: {rating['movie_id']}
                Rating: {rating['rating']:.1f}/10
                Created: {rating['created_at'].strftime('%Y-%m-%d %H:%M:%S') if rating['created_at'] else 'N/A'}
                Updated: {rating['updated_at'].strftime('%Y-%m-%d %H:%M:%S') if rating['updated_at'] else 'N/A'}"""
                
                text_widget = tk.Text(details_frame, height=8, width=50, font=('Courier', 10))
                text_widget.insert('1.0', info_text)
                text_widget.config(state='disabled')
                text_widget.pack(pady=10, fill='both', expand=True)
                
                # Close button
                ttk.Button(details_frame, text="Close", 
                          command=details_window.destroy).pack(pady=10)
            else:
                messagebox.showerror("Error", "Rating not found")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load rating details: {str(e)}")
    
    def edit_rating_dialog(self):
        """Edit rating value"""
        selection = self.ratings_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a rating to edit")
            return
        
        rating_id = self.ratings_tree.item(selection[0])['values'][0]
        current_rating = self.ratings_tree.item(selection[0])['values'][3]
        
        new_rating = tk.simpledialog.askfloat("Edit Rating", 
                                            "Enter new rating (0-10):",
                                            initialvalue=float(current_rating) if current_rating != "N/A" else 0,
                                            minvalue=0, maxvalue=10)
        
        if new_rating is None or (current_rating != "N/A" and new_rating == float(current_rating)):
            return
        
        try:
            with DbCtx() as (cur, conn):
                result = admin_update_rating(cur, rating_id, new_rating)
            
            if result:
                messagebox.showinfo("Success", "Rating updated successfully")
                self.load_all_ratings()  # Reload to show updated data
            else:
                messagebox.showerror("Error", "Rating not found")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to update rating: {str(e)}")
    
    def delete_rating(self):
        """Delete selected rating"""
        selection = self.ratings_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a rating to delete")
            return
        
        rating_id = self.ratings_tree.item(selection[0])['values'][0]
        user_id = self.ratings_tree.item(selection[0])['values'][1]
        movie_id = self.ratings_tree.item(selection[0])['values'][2]
        
        if not messagebox.askyesno("Confirm Delete", 
                                 f"Are you sure you want to delete this rating?\n\n"
                                 f"Rating ID: {rating_id}\n"
                                 f"User ID: {user_id}\n"
                                 f"Movie ID: {movie_id}\n\n"
                                 "This action cannot be undone."):
            return
        
        try:
            with DbCtx() as (cur, conn):
                result = admin_delete_rating(cur, rating_id)
            
            if result:
                messagebox.showinfo("Success", "Rating deleted successfully")
                self.load_all_ratings()
            else:
                messagebox.showerror("Error", "Rating not found")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete rating: {str(e)}")

    #===========================================
    # Add Genre to Movie Tab
    #===========================================
    def setup_movie_genres_tab(self):
        # Top frame with buttons
        top_frame = ttk.Frame(self.movie_genres_tab)
        top_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(top_frame, text="Load All Associations", command=self.load_all_movie_genres).pack(side='left', padx=5)
        ttk.Button(top_frame, text="Add Genre to Movie", command=self.add_movie_genre_dialog).pack(side='left', padx=5)
        
        # Search frame
        search_frame = ttk.LabelFrame(self.movie_genres_tab, text="Search Associations", padding=10)
        search_frame.pack(fill='x', padx=10, pady=5)
        
        # Movie search
        ttk.Label(search_frame, text="Movie:").grid(row=0, column=0, padx=5, sticky='w')
        self.movie_genre_movie_entry = ttk.Entry(search_frame, width=30)
        self.movie_genre_movie_entry.grid(row=0, column=1, padx=5, sticky='w')
        self.movie_genre_movie_entry.bind('<KeyRelease>', self.search_movies_for_genre_association)
        
        # Genre search
        ttk.Label(search_frame, text="Genre:").grid(row=1, column=0, padx=5, sticky='w')
        self.movie_genre_genre_entry = ttk.Entry(search_frame, width=30)
        self.movie_genre_genre_entry.grid(row=1, column=1, padx=5, sticky='w')
        self.movie_genre_genre_entry.bind('<KeyRelease>', self.search_genres_for_association)
        
        # Dropdowns for selection
        ttk.Label(search_frame, text="Select Movie:").grid(row=0, column=2, padx=5, sticky='w')
        self.movie_genre_movie_combobox = ttk.Combobox(search_frame, width=30, state='readonly')
        self.movie_genre_movie_combobox.grid(row=0, column=3, padx=5, sticky='w')
        
        ttk.Label(search_frame, text="Select Genre:").grid(row=1, column=2, padx=5, sticky='w')
        self.movie_genre_genre_combobox = ttk.Combobox(search_frame, width=30, state='readonly')
        self.movie_genre_genre_combobox.grid(row=1, column=3, padx=5, sticky='w')
        
        # Buttons
        ttk.Button(search_frame, text="Search by Movie", command=self.search_movie_genres_by_movie_name).grid(row=2, column=0, columnspan=2, pady=5)
        ttk.Button(search_frame, text="Search by Genre", command=self.search_movie_genres_by_genre_name).grid(row=2, column=2, columnspan=2, pady=5)
        ttk.Button(search_frame, text="Clear", command=self.clear_movie_genre_search).grid(row=2, column=4, pady=5)
        
        # Results frame with treeview
        results_frame = ttk.LabelFrame(self.movie_genres_tab, text="Movie-Genre Associations", padding=10)
        results_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Treeview with scrollbar
        tree_scroll = ttk.Scrollbar(results_frame)
        tree_scroll.pack(side='right', fill='y')
        
        self.movie_genres_tree = ttk.Treeview(results_frame, yscrollcommand=tree_scroll.set, 
                                                columns=('Movie ID', 'Movie Title', 'Genre ID', 'Genre Name'), 
                                                show='headings', height=15)
        
        # Configure columns
        self.movie_genres_tree.heading('Movie ID', text='Movie ID')
        self.movie_genres_tree.heading('Movie Title', text='Movie Title')
        self.movie_genres_tree.heading('Genre ID', text='Genre ID')
        self.movie_genres_tree.heading('Genre Name', text='Genre Name')
        
        self.movie_genres_tree.column('Movie ID', width=80)
        self.movie_genres_tree.column('Movie Title', width=250)
        self.movie_genres_tree.column('Genre ID', width=80)
        self.movie_genres_tree.column('Genre Name', width=200)
        
        self.movie_genres_tree.pack(fill='both', expand=True)
        tree_scroll.config(command=self.movie_genres_tree.yview)
        
        # Action buttons
        action_frame = ttk.Frame(self.movie_genres_tab)
        action_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(action_frame, text="Remove Association", command=self.remove_movie_genre).pack(side='left', padx=5)
        
        # Load initial data
        self.load_all_movie_genres()
    
    def search_movies_for_genre_association(self, event=None):
        """Search movies by name and populate combobox"""
        search_term = self.movie_genre_movie_entry.get().strip()
        if not search_term or len(search_term) < 2:
            self.movie_genre_movie_combobox['values'] = []
            return
        
        try:
            with DbCtx() as (cur, conn):
                movies = admin_search_movies_by_title(cur, search_term, limit=20)
            
            # Format for combobox: "ID: Title (Year)"
            movie_options = []
            for movie in movies:
                year = movie['released_date'].year if movie.get('released_date') else 'Unknown'
                movie_options.append(f"{movie['movie_id']}: {movie['title']} ({year})")
            
            self.movie_genre_movie_combobox['values'] = movie_options
            
        except Exception as e:
            # Silently fail for search-as-you-type
            pass
    
    def search_genres_for_association(self, event=None):
        """Search genres by name and populate combobox"""
        search_term = self.movie_genre_genre_entry.get().strip()
        if not search_term or len(search_term) < 2:
            self.movie_genre_genre_combobox['values'] = []
            return
        
        try:
            with DbCtx() as (cur, conn):
                genres = admin_search_genres_by_name(cur, search_term, limit=20)
            
            # Format for combobox: "ID: Genre Name"
            genre_options = [f"{g['genre_id']}: {g['genre_name']}" for g in genres]
            self.movie_genre_genre_combobox['values'] = genre_options
            
        except Exception as e:
            # Silently fail for search-as-you-type
            pass
    
    def load_all_movie_genres(self):
        """Load all movie-genre associations with movie and genre names"""
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mg.movie_id, m.title, mg.genre_id, g.genre_name
                FROM movie_genres mg
                JOIN movies m ON mg.movie_id = m.movie_id
                JOIN genres g ON mg.genre_id = g.genre_id
                ORDER BY m.title, g.genre_name
                LIMIT 500
                """
                cur.execute(query)
                rows = cur.fetchall()
            
            self.update_movie_genres_tree(rows)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load movie-genre associations: {str(e)}")
    
    def update_movie_genres_tree(self, rows):
        """Update treeview with movie-genre associations"""
        # Clear existing items
        for item in self.movie_genres_tree.get_children():
            self.movie_genres_tree.delete(item)
        
        # Add associations
        for row in rows:
            self.movie_genres_tree.insert('', 'end', values=(
                row[0],  # movie_id
                row[1],  # movie_title
                row[2],  # genre_id
                row[3]   # genre_name
            ))
    
    def search_movie_genres_by_movie_name(self):
        """Search associations by movie name"""
        selected_movie = self.movie_genre_movie_combobox.get()
        if not selected_movie:
            messagebox.showwarning("Warning", "Please select a movie from the dropdown")
            return
        
        # Extract movie ID from combobox selection ("ID: Title (Year)")
        movie_id = int(selected_movie.split(':')[0])
        movie_title = selected_movie.split(':')[1].split('(')[0].strip()
        
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mg.movie_id, m.title, mg.genre_id, g.genre_name
                FROM movie_genres mg
                JOIN movies m ON mg.movie_id = m.movie_id
                JOIN genres g ON mg.genre_id = g.genre_id
                WHERE mg.movie_id = %s
                ORDER BY g.genre_name
                """
                cur.execute(query, (movie_id,))
                rows = cur.fetchall()
            
            self.update_movie_genres_tree(rows)
            
            if not rows:
                messagebox.showinfo("No Results", f"Movie '{movie_title}' has no genre associations")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def search_movie_genres_by_genre_name(self):
        """Search associations by genre name"""
        selected_genre = self.movie_genre_genre_combobox.get()
        if not selected_genre:
            messagebox.showwarning("Warning", "Please select a genre from the dropdown")
            return
        
        # Extract genre ID from combobox selection ("ID: Genre Name")
        genre_id = int(selected_genre.split(':')[0])
        genre_name = selected_genre.split(':')[1].strip()
        
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mg.movie_id, m.title, mg.genre_id, g.genre_name
                FROM movie_genres mg
                JOIN movies m ON mg.movie_id = m.movie_id
                JOIN genres g ON mg.genre_id = g.genre_id
                WHERE mg.genre_id = %s
                ORDER BY m.title
                """
                cur.execute(query, (genre_id,))
                rows = cur.fetchall()
            
            self.update_movie_genres_tree(rows)
            
            if not rows:
                messagebox.showinfo("No Results", f"Genre '{genre_name}' has no movie associations")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def clear_movie_genre_search(self):
        """Clear search fields and reload all associations"""
        self.movie_genre_movie_entry.delete(0, tk.END)
        self.movie_genre_genre_entry.delete(0, tk.END)
        self.movie_genre_movie_combobox.set('')
        self.movie_genre_genre_combobox.set('')
        self.movie_genre_movie_combobox['values'] = []
        self.movie_genre_genre_combobox['values'] = []
        self.load_all_movie_genres()
    
    def add_movie_genre_dialog(self):
        """Add a genre to a movie using names"""
        # Create dialog window
        dialog = tk.Toplevel(self.root)
        dialog.title("Add Genre to Movie")
        dialog.geometry("500x350")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Movie selection frame
        movie_frame = ttk.LabelFrame(dialog, text="Select Movie", padding=10)
        movie_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(movie_frame, text="Search Movie:").pack(anchor='w')
        movie_search_entry = ttk.Entry(movie_frame, width=50)
        movie_search_entry.pack(fill='x', pady=5)
        movie_search_entry.focus()
        
        ttk.Label(movie_frame, text="Select:").pack(anchor='w')
        movie_combobox = ttk.Combobox(movie_frame, width=50, state='readonly')
        movie_combobox.pack(fill='x', pady=5)
        
        # Genre selection frame
        genre_frame = ttk.LabelFrame(dialog, text="Select Genre", padding=10)
        genre_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(genre_frame, text="Search Genre:").pack(anchor='w')
        genre_search_entry = ttk.Entry(genre_frame, width=50)
        genre_search_entry.pack(fill='x', pady=5)
        
        ttk.Label(genre_frame, text="Select:").pack(anchor='w')
        genre_combobox = ttk.Combobox(genre_frame, width=50, state='readonly')
        genre_combobox.pack(fill='x', pady=5)
        
        # Buttons
        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(button_frame, text="Add Association", 
                  command=lambda: self.add_genre_association_from_dialog(
                      movie_combobox.get(), genre_combobox.get(), dialog)).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Cancel", 
                  command=dialog.destroy).pack(side='left', padx=5)
        
        # Bind search functionality
        def search_movies(event=None):
            search_term = movie_search_entry.get().strip()
            if len(search_term) >= 2:
                try:
                    with DbCtx() as (cur, conn):
                        movies = admin_search_movies_by_title(cur, search_term, limit=20)
                    options = [f"{m['movie_id']}: {m['title']} ({m.get('released_date', 'Unknown')})" for m in movies]
                    movie_combobox['values'] = options
                except:
                    pass
        
        def search_genres(event=None):
            search_term = genre_search_entry.get().strip()
            if len(search_term) >= 2:
                try:
                    with DbCtx() as (cur, conn):
                        genres = admin_search_genres_by_name(cur, search_term, limit=20)
                    options = [f"{g['genre_id']}: {g['genre_name']}" for g in genres]
                    genre_combobox['values'] = options
                except:
                    pass
        
        movie_search_entry.bind('<KeyRelease>', search_movies)
        genre_search_entry.bind('<KeyRelease>', search_genres)
    
    def add_genre_association_from_dialog(self, movie_selection, genre_selection, dialog):
        """Add genre association from dialog selections"""
        if not movie_selection or not genre_selection:
            messagebox.showwarning("Warning", "Please select both a movie and a genre")
            return
        
        try:
            # Extract IDs from selections
            movie_id = int(movie_selection.split(':')[0])
            genre_id = int(genre_selection.split(':')[0])
            
            with DbCtx() as (cur, conn):
                # Check if association already exists
                existing = admin_get_movie_genre(cur, movie_id, genre_id)
                if existing:
                    messagebox.showwarning("Warning", "This movie already has this genre")
                    return
                
                # Create association
                result = admin_create_movie_genre(cur, movie_id, genre_id)
            
            if result:
                messagebox.showinfo("Success", "Genre added to movie successfully")
                dialog.destroy()
                self.load_all_movie_genres()
            else:
                messagebox.showerror("Error", "Failed to add genre to movie")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to add genre to movie: {str(e)}")
    
    def remove_movie_genre(self):
        """Remove selected movie-genre association"""
        selection = self.movie_genres_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select an association to remove")
            return
        
        movie_id = self.movie_genres_tree.item(selection[0])['values'][0]
        movie_title = self.movie_genres_tree.item(selection[0])['values'][1]
        genre_id = self.movie_genres_tree.item(selection[0])['values'][2]
        genre_name = self.movie_genres_tree.item(selection[0])['values'][3]
        
        if not messagebox.askyesno("Confirm Remove", 
                                 f"Are you sure you want to remove this association?\n\n"
                                 f"Movie: {movie_title}\n"
                                 f"Genre: {genre_name}\n\n"
                                 "This will remove the genre from the movie."):
            return
        
        try:
            with DbCtx() as (cur, conn):
                result = admin_delete_movie_genre(cur, movie_id, genre_id)
            
            if result:
                messagebox.showinfo("Success", "Genre removed from movie successfully")
                self.load_all_movie_genres()
            else:
                messagebox.showerror("Error", "Association not found")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to remove association: {str(e)}")

    #===========================================
    # Add Company to Movie Tab
    #===========================================

    def setup_movie_companies_tab(self):
        # Top frame with buttons
        top_frame = ttk.Frame(self.movie_companies_tab)
        top_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(top_frame, text="Load All Associations", command=self.load_all_movie_companies).pack(side='left', padx=5)
        ttk.Button(top_frame, text="Add Company to Movie", command=self.add_movie_company_dialog).pack(side='left', padx=5)
        
        # Search frame
        search_frame = ttk.LabelFrame(self.movie_companies_tab, text="Search Associations", padding=10)
        search_frame.pack(fill='x', padx=10, pady=5)
        
        # Movie search
        ttk.Label(search_frame, text="Movie:").grid(row=0, column=0, padx=5, sticky='w')
        self.movie_company_movie_entry = ttk.Entry(search_frame, width=30)
        self.movie_company_movie_entry.grid(row=0, column=1, padx=5, sticky='w')
        self.movie_company_movie_entry.bind('<KeyRelease>', self.search_movies_for_company_association)
        
        # Company search
        ttk.Label(search_frame, text="Company:").grid(row=1, column=0, padx=5, sticky='w')
        self.movie_company_company_entry = ttk.Entry(search_frame, width=30)
        self.movie_company_company_entry.grid(row=1, column=1, padx=5, sticky='w')
        self.movie_company_company_entry.bind('<KeyRelease>', self.search_companies_for_association)
        
        # Dropdowns for selection
        ttk.Label(search_frame, text="Select Movie:").grid(row=0, column=2, padx=5, sticky='w')
        self.movie_company_movie_combobox = ttk.Combobox(search_frame, width=30, state='readonly')
        self.movie_company_movie_combobox.grid(row=0, column=3, padx=5, sticky='w')
        
        ttk.Label(search_frame, text="Select Company:").grid(row=1, column=2, padx=5, sticky='w')
        self.movie_company_company_combobox = ttk.Combobox(search_frame, width=30, state='readonly')
        self.movie_company_company_combobox.grid(row=1, column=3, padx=5, sticky='w')
        
        # Buttons
        ttk.Button(search_frame, text="Search by Movie", command=self.search_movie_companies_by_movie_name).grid(row=2, column=0, columnspan=2, pady=5)
        ttk.Button(search_frame, text="Search by Company", command=self.search_movie_companies_by_company_name).grid(row=2, column=2, columnspan=2, pady=5)
        ttk.Button(search_frame, text="Clear", command=self.clear_movie_company_search).grid(row=2, column=4, pady=5)
        
        # Results frame with treeview
        results_frame = ttk.LabelFrame(self.movie_companies_tab, text="Movie-Production Company Associations", padding=10)
        results_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Treeview with scrollbar
        tree_scroll = ttk.Scrollbar(results_frame)
        tree_scroll.pack(side='right', fill='y')
        
        self.movie_companies_tree = ttk.Treeview(results_frame, yscrollcommand=tree_scroll.set, 
                                                columns=('Movie ID', 'Movie Title', 'Company ID', 'Company Name'), 
                                                show='headings', height=15)
        
        # Configure columns
        self.movie_companies_tree.heading('Movie ID', text='Movie ID')
        self.movie_companies_tree.heading('Movie Title', text='Movie Title')
        self.movie_companies_tree.heading('Company ID', text='Company ID')
        self.movie_companies_tree.heading('Company Name', text='Company Name')
        
        self.movie_companies_tree.column('Movie ID', width=80)
        self.movie_companies_tree.column('Movie Title', width=250)
        self.movie_companies_tree.column('Company ID', width=80)
        self.movie_companies_tree.column('Company Name', width=200)
        
        self.movie_companies_tree.pack(fill='both', expand=True)
        tree_scroll.config(command=self.movie_companies_tree.yview)
        
        # Action buttons
        action_frame = ttk.Frame(self.movie_companies_tab)
        action_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(action_frame, text="Remove Association", command=self.remove_movie_company).pack(side='left', padx=5)
        
        # Load initial data
        self.load_all_movie_companies()
    
    def search_movies_for_company_association(self, event=None):
        """Search movies by name and populate combobox"""
        search_term = self.movie_company_movie_entry.get().strip()
        if not search_term or len(search_term) < 2:
            self.movie_company_movie_combobox['values'] = []
            return
        
        try:
            with DbCtx() as (cur, conn):
                movies = admin_search_movies_by_title(cur, search_term, limit=20)
            
            # Format for combobox: "ID: Title (Year)"
            movie_options = []
            for movie in movies:
                year = movie['released_date'].year if movie.get('released_date') else 'Unknown'
                movie_options.append(f"{movie['movie_id']}: {movie['title']} ({year})")
            
            self.movie_company_movie_combobox['values'] = movie_options
            
        except Exception as e:
            # Silently fail for search-as-you-type
            pass
    
    def search_companies_for_association(self, event=None):
        """Search companies by name and populate combobox"""
        search_term = self.movie_company_company_entry.get().strip()
        if not search_term or len(search_term) < 2:
            self.movie_company_company_combobox['values'] = []
            return
        
        try:
            with DbCtx() as (cur, conn):
                companies = admin_search_companies_by_name(cur, search_term, limit=20)
            
            # Format for combobox: "ID: Company Name"
            company_options = [f"{c['company_id']}: {c['name']}" for c in companies]
            self.movie_company_company_combobox['values'] = company_options
            
        except Exception as e:
            # Silently fail for search-as-you-type
            pass
    
    def load_all_movie_companies(self):
        """Load all movie-company associations with movie and company names"""
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mpc.movie_id, m.title, mpc.company_id, pc.name
                FROM movie_production_companies mpc
                JOIN movies m ON mpc.movie_id = m.movie_id
                JOIN production_companies pc ON mpc.company_id = pc.company_id
                ORDER BY m.title, pc.name
                LIMIT 500
                """
                cur.execute(query)
                rows = cur.fetchall()
            
            self.update_movie_companies_tree(rows)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load movie-company associations: {str(e)}")
    
    def update_movie_companies_tree(self, rows):
        """Update treeview with movie-company associations"""
        # Clear existing items
        for item in self.movie_companies_tree.get_children():
            self.movie_companies_tree.delete(item)
        
        # Add associations
        for row in rows:
            self.movie_companies_tree.insert('', 'end', values=(
                row[0],  # movie_id
                row[1],  # movie_title
                row[2],  # company_id
                row[3]   # company_name
            ))
    
    def search_movie_companies_by_movie_name(self):
        """Search associations by movie name"""
        selected_movie = self.movie_company_movie_combobox.get()
        if not selected_movie:
            messagebox.showwarning("Warning", "Please select a movie from the dropdown")
            return
        
        # Extract movie ID from combobox selection ("ID: Title (Year)")
        movie_id = int(selected_movie.split(':')[0])
        movie_title = selected_movie.split(':')[1].split('(')[0].strip()
        
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mpc.movie_id, m.title, mpc.company_id, pc.name
                FROM movie_production_companies mpc
                JOIN movies m ON mpc.movie_id = m.movie_id
                JOIN production_companies pc ON mpc.company_id = pc.company_id
                WHERE mpc.movie_id = %s
                ORDER BY pc.name
                """
                cur.execute(query, (movie_id,))
                rows = cur.fetchall()
            
            self.update_movie_companies_tree(rows)
            
            if not rows:
                messagebox.showinfo("No Results", f"Movie '{movie_title}' has no production company associations")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def search_movie_companies_by_company_name(self):
        """Search associations by company name"""
        selected_company = self.movie_company_company_combobox.get()
        if not selected_company:
            messagebox.showwarning("Warning", "Please select a company from the dropdown")
            return
        
        # Extract company ID from combobox selection ("ID: Company Name")
        company_id = int(selected_company.split(':')[0])
        company_name = selected_company.split(':')[1].strip()
        
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mpc.movie_id, m.title, mpc.company_id, pc.name
                FROM movie_production_companies mpc
                JOIN movies m ON mpc.movie_id = m.movie_id
                JOIN production_companies pc ON mpc.company_id = pc.company_id
                WHERE mpc.company_id = %s
                ORDER BY m.title
                """
                cur.execute(query, (company_id,))
                rows = cur.fetchall()
            
            self.update_movie_companies_tree(rows)
            
            if not rows:
                messagebox.showinfo("No Results", f"Company '{company_name}' has no movie associations")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def clear_movie_company_search(self):
        """Clear search fields and reload all associations"""
        self.movie_company_movie_entry.delete(0, tk.END)
        self.movie_company_company_entry.delete(0, tk.END)
        self.movie_company_movie_combobox.set('')
        self.movie_company_company_combobox.set('')
        self.movie_company_movie_combobox['values'] = []
        self.movie_company_company_combobox['values'] = []
        self.load_all_movie_companies()
    
    def add_movie_company_dialog(self):
        """Add a production company to a movie using names"""
        # Create dialog window
        dialog = tk.Toplevel(self.root)
        dialog.title("Add Production Company to Movie")
        dialog.geometry("500x350")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Movie selection frame
        movie_frame = ttk.LabelFrame(dialog, text="Select Movie", padding=10)
        movie_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(movie_frame, text="Search Movie:").pack(anchor='w')
        movie_search_entry = ttk.Entry(movie_frame, width=50)
        movie_search_entry.pack(fill='x', pady=5)
        movie_search_entry.focus()
        
        ttk.Label(movie_frame, text="Select:").pack(anchor='w')
        movie_combobox = ttk.Combobox(movie_frame, width=50, state='readonly')
        movie_combobox.pack(fill='x', pady=5)
        
        # Company selection frame
        company_frame = ttk.LabelFrame(dialog, text="Select Production Company", padding=10)
        company_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(company_frame, text="Search Company:").pack(anchor='w')
        company_search_entry = ttk.Entry(company_frame, width=50)
        company_search_entry.pack(fill='x', pady=5)
        
        ttk.Label(company_frame, text="Select:").pack(anchor='w')
        company_combobox = ttk.Combobox(company_frame, width=50, state='readonly')
        company_combobox.pack(fill='x', pady=5)
        
        # Buttons
        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(button_frame, text="Add Association", 
                  command=lambda: self.add_company_association_from_dialog(
                      movie_combobox.get(), company_combobox.get(), dialog)).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Cancel", 
                  command=dialog.destroy).pack(side='left', padx=5)
        
        # Bind search functionality
        def search_movies(event=None):
            search_term = movie_search_entry.get().strip()
            if len(search_term) >= 2:
                try:
                    with DbCtx() as (cur, conn):
                        movies = admin_search_movies_by_title(cur, search_term, limit=20)
                    options = [f"{m['movie_id']}: {m['title']} ({m.get('released_date', 'Unknown')})" for m in movies]
                    movie_combobox['values'] = options
                except:
                    pass
        
        def search_companies(event=None):
            search_term = company_search_entry.get().strip()
            if len(search_term) >= 2:
                try:
                    with DbCtx() as (cur, conn):
                        companies = admin_search_companies_by_name(cur, search_term, limit=20)
                    options = [f"{c['company_id']}: {c['name']}" for c in companies]
                    company_combobox['values'] = options
                except:
                    pass
        
        movie_search_entry.bind('<KeyRelease>', search_movies)
        company_search_entry.bind('<KeyRelease>', search_companies)
    
    def add_company_association_from_dialog(self, movie_selection, company_selection, dialog):
        """Add company association from dialog selections"""
        if not movie_selection or not company_selection:
            messagebox.showwarning("Warning", "Please select both a movie and a production company")
            return
        
        try:
            # Extract IDs from selections
            movie_id = int(movie_selection.split(':')[0])
            company_id = int(company_selection.split(':')[0])
            
            with DbCtx() as (cur, conn):
                # Check if association already exists
                existing = admin_get_movie_company(cur, movie_id, company_id)
                if existing:
                    messagebox.showwarning("Warning", "This movie already has this production company")
                    return
                
                # Create association
                result = admin_create_movie_company(cur, movie_id, company_id)
            
            if result:
                messagebox.showinfo("Success", "Production company added to movie successfully")
                dialog.destroy()
                self.load_all_movie_companies()
            else:
                messagebox.showerror("Error", "Failed to add production company to movie")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to add production company to movie: {str(e)}")
    
    def remove_movie_company(self):
        """Remove selected movie-company association"""
        selection = self.movie_companies_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select an association to remove")
            return
        
        movie_id = self.movie_companies_tree.item(selection[0])['values'][0]
        movie_title = self.movie_companies_tree.item(selection[0])['values'][1]
        company_id = self.movie_companies_tree.item(selection[0])['values'][2]
        company_name = self.movie_companies_tree.item(selection[0])['values'][3]
        
        if not messagebox.askyesno("Confirm Remove", 
                                 f"Are you sure you want to remove this association?\n\n"
                                 f"Movie: {movie_title}\n"
                                 f"Company: {company_name}\n\n"
                                 "This will remove the production company from the movie."):
            return
        
        try:
            with DbCtx() as (cur, conn):
                result = admin_delete_movie_company(cur, movie_id, company_id)
            
            if result:
                messagebox.showinfo("Success", "Production company removed from movie successfully")
                self.load_all_movie_companies()
            else:
                messagebox.showerror("Error", "Association not found")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to remove association: {str(e)}")

    #===========================================
    # People Tab
    #===========================================

    def setup_people_tab(self):
        # Top frame with buttons
        top_frame = ttk.Frame(self.people_tab)
        top_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(top_frame, text="Load All People", command=self.load_all_people).pack(side='left', padx=5)
        ttk.Button(top_frame, text="Create New Person", command=self.create_person_dialog).pack(side='left', padx=5)
        
        # Search frame
        search_frame = ttk.LabelFrame(self.people_tab, text="Search People", padding=10)
        search_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(search_frame, text="Name:").grid(row=0, column=0, padx=5, sticky='w')
        self.people_search_entry = ttk.Entry(search_frame, width=40)
        self.people_search_entry.grid(row=0, column=1, padx=5)
        self.people_search_entry.bind('<Return>', lambda e: self.search_people())
        
        ttk.Button(search_frame, text="Search", command=self.search_people).grid(row=0, column=2, padx=5)
        ttk.Button(search_frame, text="Clear", command=self.clear_people_search).grid(row=0, column=3, padx=5)
        
        # Results frame with treeview
        results_frame = ttk.LabelFrame(self.people_tab, text="People", padding=10)
        results_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Treeview with scrollbar
        tree_scroll = ttk.Scrollbar(results_frame)
        tree_scroll.pack(side='right', fill='y')
        
        self.people_tree = ttk.Treeview(results_frame, yscrollcommand=tree_scroll.set, 
                                       columns=('ID', 'Name', 'TMDB ID', 'Gender', 'Profile Path'), 
                                       show='headings', height=15)
        
        # Configure columns
        self.people_tree.heading('ID', text='ID')
        self.people_tree.heading('Name', text='Name')
        self.people_tree.heading('TMDB ID', text='TMDB ID')
        self.people_tree.heading('Gender', text='Gender')
        self.people_tree.heading('Profile Path', text='Profile Path')
        
        self.people_tree.column('ID', width=80)
        self.people_tree.column('Name', width=200)
        self.people_tree.column('TMDB ID', width=100)
        self.people_tree.column('Gender', width=100)
        self.people_tree.column('Profile Path', width=200)
        
        self.people_tree.pack(fill='both', expand=True)
        tree_scroll.config(command=self.people_tree.yview)
        
        # Double-click to view details
        self.people_tree.bind('<Double-1>', lambda e: self.view_person_details())
        
        # Action buttons
        action_frame = ttk.Frame(self.people_tab)
        action_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(action_frame, text="View Details", command=self.view_person_details).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Edit Person", command=self.edit_person_dialog).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Delete Person", command=self.delete_person).pack(side='left', padx=5)
        
        # Load initial data
        self.load_all_people()
    
    def load_all_people(self):
        """Load all people with pagination"""
        try:
            with DbCtx() as (cur, conn):
                people = admin_read_people(cur, limit=200)
            
            self.update_people_tree(people)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load people: {str(e)}")
    
    def update_people_tree(self, people):
        """Update treeview with people data"""
        # Clear existing items
        for item in self.people_tree.get_children():
            self.people_tree.delete(item)
        
        # Add people
        for person in people:
            self.people_tree.insert('', 'end', values=(
                person['person_id'],
                person['name'],
                person['tmdb_id'] or "N/A",
                person['gender_display'],
                person['profile_path'] or "N/A"
            ))
    
    def search_people(self):
        """Search people by name"""
        search_term = self.people_search_entry.get().strip()
        if not search_term:
            messagebox.showwarning("Warning", "Please enter a search term")
            return
        
        try:
            with DbCtx() as (cur, conn):
                results = admin_search_people_by_name(cur, search_term)
            
            self.update_people_tree(results)
            
            if not results:
                messagebox.showinfo("No Results", f"No people found matching '{search_term}'")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def clear_people_search(self):
        """Clear search and reload all people"""
        self.people_search_entry.delete(0, tk.END)
        self.load_all_people()
    
    def view_person_details(self):
        """Show detailed person information"""
        selection = self.people_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a person to view details")
            return
        
        person_id = self.people_tree.item(selection[0])['values'][0]
        
        try:
            with DbCtx() as (cur, conn):
                person = admin_get_person(cur, person_id)
            
            if person:
                # Create details dialog
                details_window = tk.Toplevel(self.root)
                details_window.title(f"Person Details: {person['name']}")
                details_window.geometry("500x350")
                details_window.transient(self.root)
                details_window.grab_set()
                
                # Details frame
                details_frame = ttk.LabelFrame(details_window, text="Person Information", padding=15)
                details_frame.pack(fill='both', expand=True, padx=10, pady=10)
                
                # Person info
                info_text = f"""Person ID: {person['person_id']}
                Name: {person['name']}
                TMDB ID: {person['tmdb_id'] or 'N/A'}
                Gender: {person['gender_display']} (Code: {person['gender']})
                Profile Path: {person['profile_path'] or 'N/A'}"""
                                
                text_widget = tk.Text(details_frame, height=10, width=50, font=('Courier', 10))
                text_widget.insert('1.0', info_text)
                text_widget.config(state='disabled')
                text_widget.pack(pady=10, fill='both', expand=True)
                
                # Close button
                ttk.Button(details_frame, text="Close", 
                          command=details_window.destroy).pack(pady=10)
            else:
                messagebox.showerror("Error", "Person not found")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load person details: {str(e)}")
    
    def create_person_dialog(self):
        """Create a new person"""
        # Create dialog window
        dialog = tk.Toplevel(self.root)
        dialog.title("Create New Person")
        dialog.geometry("500x400")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Form frame
        form_frame = ttk.LabelFrame(dialog, text="Person Details", padding=15)
        form_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Name
        ttk.Label(form_frame, text="Name *:").grid(row=0, column=0, sticky='w', pady=5)
        name_entry = ttk.Entry(form_frame, width=40)
        name_entry.grid(row=0, column=1, sticky='w', pady=5, padx=5)
        name_entry.focus()
        
        # TMDB ID
        ttk.Label(form_frame, text="TMDB ID:").grid(row=1, column=0, sticky='w', pady=5)
        tmdb_entry = ttk.Entry(form_frame, width=40)
        tmdb_entry.grid(row=1, column=1, sticky='w', pady=5, padx=5)
        
        # Gender
        ttk.Label(form_frame, text="Gender:").grid(row=2, column=0, sticky='w', pady=5)
        gender_var = tk.StringVar(value="0")
        gender_frame = ttk.Frame(form_frame)
        gender_frame.grid(row=2, column=1, sticky='w', pady=5, padx=5)
        
        ttk.Radiobutton(gender_frame, text="Unknown", variable=gender_var, value="0").pack(side='left')
        ttk.Radiobutton(gender_frame, text="Female", variable=gender_var, value="1").pack(side='left', padx=10)
        ttk.Radiobutton(gender_frame, text="Male", variable=gender_var, value="2").pack(side='left')
        ttk.Radiobutton(gender_frame, text="Non-binary", variable=gender_var, value="3").pack(side='left', padx=10)
        
        # Profile Path
        ttk.Label(form_frame, text="Profile Path:").grid(row=3, column=0, sticky='w', pady=5)
        profile_entry = ttk.Entry(form_frame, width=40)
        profile_entry.grid(row=3, column=1, sticky='w', pady=5, padx=5)
        
        # Buttons
        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(button_frame, text="Create Person", 
                  command=lambda: self.create_person_from_dialog(
                      name_entry.get().strip(),
                      tmdb_entry.get().strip(),
                      gender_var.get(),
                      profile_entry.get().strip(),
                      dialog
                  )).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Cancel", 
                  command=dialog.destroy).pack(side='left', padx=5)
    
    def create_person_from_dialog(self, name, tmdb_id, gender, profile_path, dialog):
        """Create person from dialog data"""
        if not name:
            messagebox.showwarning("Warning", "Name is required")
            return
        
        try:
            # Convert inputs
            tmdb_id_int = int(tmdb_id) if tmdb_id else None
            gender_int = int(gender) if gender else None
            profile_path = profile_path if profile_path else None
            
            with DbCtx() as (cur, conn):
                person_id = admin_create_person(cur, tmdb_id_int, name, gender_int, profile_path)
            
            if person_id:
                messagebox.showinfo("Success", f"Person '{name}' created successfully with ID: {person_id}")
                dialog.destroy()
                self.load_all_people()
            else:
                messagebox.showerror("Error", "Failed to create person")
                
        except ValueError:
            messagebox.showerror("Input Error", "TMDB ID must be a valid number")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to create person: {str(e)}")
    
    def edit_person_dialog(self):
        """Edit selected person"""
        selection = self.people_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a person to edit")
            return
        
        person_id = self.people_tree.item(selection[0])['values'][0]
        current_name = self.people_tree.item(selection[0])['values'][1]
        
        try:
            with DbCtx() as (cur, conn):
                person = admin_get_person(cur, person_id)
            
            if not person:
                messagebox.showerror("Error", "Person not found")
                return
            
            # Create edit dialog
            dialog = tk.Toplevel(self.root)
            dialog.title(f"Edit Person: {current_name}")
            dialog.geometry("500x400")
            dialog.transient(self.root)
            dialog.grab_set()
            
            # Form frame
            form_frame = ttk.LabelFrame(dialog, text="Edit Person Details", padding=15)
            form_frame.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Name
            ttk.Label(form_frame, text="Name *:").grid(row=0, column=0, sticky='w', pady=5)
            name_entry = ttk.Entry(form_frame, width=40)
            name_entry.insert(0, person['name'])
            name_entry.grid(row=0, column=1, sticky='w', pady=5, padx=5)
            name_entry.focus()
            
            # TMDB ID
            ttk.Label(form_frame, text="TMDB ID:").grid(row=1, column=0, sticky='w', pady=5)
            tmdb_entry = ttk.Entry(form_frame, width=40)
            tmdb_entry.insert(0, str(person['tmdb_id']) if person['tmdb_id'] else "")
            tmdb_entry.grid(row=1, column=1, sticky='w', pady=5, padx=5)
            
            # Gender
            ttk.Label(form_frame, text="Gender:").grid(row=2, column=0, sticky='w', pady=5)
            gender_var = tk.StringVar(value=str(person['gender']))
            gender_frame = ttk.Frame(form_frame)
            gender_frame.grid(row=2, column=1, sticky='w', pady=5, padx=5)
            
            ttk.Radiobutton(gender_frame, text="Unknown", variable=gender_var, value="0").pack(side='left')
            ttk.Radiobutton(gender_frame, text="Female", variable=gender_var, value="1").pack(side='left', padx=10)
            ttk.Radiobutton(gender_frame, text="Male", variable=gender_var, value="2").pack(side='left')
            ttk.Radiobutton(gender_frame, text="Non-binary", variable=gender_var, value="3").pack(side='left', padx=10)
            
            # Profile Path
            ttk.Label(form_frame, text="Profile Path:").grid(row=3, column=0, sticky='w', pady=5)
            profile_entry = ttk.Entry(form_frame, width=40)
            profile_entry.insert(0, person['profile_path'] or "")
            profile_entry.grid(row=3, column=1, sticky='w', pady=5, padx=5)
            
            # Buttons
            button_frame = ttk.Frame(dialog)
            button_frame.pack(fill='x', padx=10, pady=10)
            
            ttk.Button(button_frame, text="Save Changes", 
                      command=lambda: self.update_person_from_dialog(
                          person_id,
                          name_entry.get().strip(),
                          tmdb_entry.get().strip(),
                          gender_var.get(),
                          profile_entry.get().strip(),
                          dialog
                      )).pack(side='left', padx=5)
            ttk.Button(button_frame, text="Cancel", 
                      command=dialog.destroy).pack(side='left', padx=5)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load person for editing: {str(e)}")
    
    def update_person_from_dialog(self, person_id, name, tmdb_id, gender, profile_path, dialog):
        """Update person from edit dialog"""
        if not name:
            messagebox.showwarning("Warning", "Name is required")
            return
        
        try:
            update_data = {}
            
            # Only include fields that changed
            if name:
                update_data['name'] = name
            
            if tmdb_id:
                update_data['tmdb_id'] = int(tmdb_id)
            else:
                update_data['tmdb_id'] = None
            
            update_data['gender'] = int(gender)
            update_data['profile_path'] = profile_path if profile_path else None
            
            with DbCtx() as (cur, conn):
                result = admin_update_person(cur, person_id, update_data)
            
            if result:
                messagebox.showinfo("Success", "Person updated successfully")
                dialog.destroy()
                self.load_all_people()
            else:
                messagebox.showerror("Error", "Person not found")
                
        except ValueError:
            messagebox.showerror("Input Error", "TMDB ID must be a valid number")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to update person: {str(e)}")
    
    def delete_person(self):
        """Delete selected person"""
        selection = self.people_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a person to delete")
            return
        
        person_id = self.people_tree.item(selection[0])['values'][0]
        person_name = self.people_tree.item(selection[0])['values'][1]
        
        if not messagebox.askyesno("Confirm Delete", 
                                 f"Are you sure you want to delete '{person_name}'?\n\n"
                                 "This action cannot be undone."):
            return
        
        try:
            with DbCtx() as (cur, conn):
                result = admin_delete_person(cur, person_id)
            
            if result:
                messagebox.showinfo("Success", "Person deleted successfully")
                self.load_all_people()
            else:
                messagebox.showerror("Error", "Person not found")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete person: {str(e)}")

    #===========================================
    # Movie Cast Tab
    #===========================================

    def setup_movie_cast_tab(self):
        # Top frame with buttons
        top_frame = ttk.Frame(self.movie_cast_tab)
        top_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(top_frame, text="Load All Cast", command=self.load_all_movie_casts).pack(side='left', padx=5)
        ttk.Button(top_frame, text="Add Cast Member", command=self.add_cast_member_dialog).pack(side='left', padx=5)
        
        # Search frame
        search_frame = ttk.LabelFrame(self.movie_cast_tab, text="Search Cast", padding=10)
        search_frame.pack(fill='x', padx=10, pady=5)
        
        # Movie search
        ttk.Label(search_frame, text="Movie:").grid(row=0, column=0, padx=5, sticky='w')
        self.cast_movie_entry = ttk.Entry(search_frame, width=30)
        self.cast_movie_entry.grid(row=0, column=1, padx=5, sticky='w')
        self.cast_movie_entry.bind('<KeyRelease>', self.search_movies_for_cast)
        
        # Person search
        ttk.Label(search_frame, text="Person:").grid(row=1, column=0, padx=5, sticky='w')
        self.cast_person_entry = ttk.Entry(search_frame, width=30)
        self.cast_person_entry.grid(row=1, column=1, padx=5, sticky='w')
        self.cast_person_entry.bind('<KeyRelease>', self.search_people_for_cast)
        
        # Character search
        ttk.Label(search_frame, text="Character:").grid(row=2, column=0, padx=5, sticky='w')
        self.cast_character_entry = ttk.Entry(search_frame, width=30)
        self.cast_character_entry.grid(row=2, column=1, padx=5, sticky='w')
        
        # Dropdowns for selection
        ttk.Label(search_frame, text="Select Movie:").grid(row=0, column=2, padx=5, sticky='w')
        self.cast_movie_combobox = ttk.Combobox(search_frame, width=30, state='readonly')
        self.cast_movie_combobox.grid(row=0, column=3, padx=5, sticky='w')
        
        ttk.Label(search_frame, text="Select Person:").grid(row=1, column=2, padx=5, sticky='w')
        self.cast_person_combobox = ttk.Combobox(search_frame, width=30, state='readonly')
        self.cast_person_combobox.grid(row=1, column=3, padx=5, sticky='w')
        
        # Buttons
        ttk.Button(search_frame, text="Search by Movie", command=self.search_cast_by_movie).grid(row=3, column=0, columnspan=2, pady=5)
        ttk.Button(search_frame, text="Search by Person", command=self.search_cast_by_person).grid(row=3, column=2, columnspan=2, pady=5)
        ttk.Button(search_frame, text="Search by Character", command=self.search_cast_by_character).grid(row=4, column=0, columnspan=2, pady=5)
        ttk.Button(search_frame, text="Clear", command=self.clear_cast_search).grid(row=4, column=2, columnspan=2, pady=5)
        
        # Results frame with treeview
        results_frame = ttk.LabelFrame(self.movie_cast_tab, text="Movie Cast", padding=10)
        results_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Treeview with scrollbar
        tree_scroll = ttk.Scrollbar(results_frame)
        tree_scroll.pack(side='right', fill='y')
        
        self.cast_tree = ttk.Treeview(results_frame, yscrollcommand=tree_scroll.set, 
                                     columns=('Movie ID', 'Movie Title', 'Person ID', 'Person Name', 'Character', 'Cast Order', 'Credit ID'), 
                                     show='headings', height=15)
        
        # Configure columns
        self.cast_tree.heading('Movie ID', text='Movie ID')
        self.cast_tree.heading('Movie Title', text='Movie Title')
        self.cast_tree.heading('Person ID', text='Person ID')
        self.cast_tree.heading('Person Name', text='Person Name')
        self.cast_tree.heading('Character', text='Character')
        self.cast_tree.heading('Cast Order', text='Cast Order')
        self.cast_tree.heading('Credit ID', text='Credit ID')
        
        self.cast_tree.column('Movie ID', width=80)
        self.cast_tree.column('Movie Title', width=200)
        self.cast_tree.column('Person ID', width=80)
        self.cast_tree.column('Person Name', width=150)
        self.cast_tree.column('Character', width=150)
        self.cast_tree.column('Cast Order', width=80)
        self.cast_tree.column('Credit ID', width=150)
        
        self.cast_tree.pack(fill='both', expand=True)
        tree_scroll.config(command=self.cast_tree.yview)
        
        # Double-click to view details
        self.cast_tree.bind('<Double-1>', lambda e: self.view_cast_details())
        
        # Action buttons
        action_frame = ttk.Frame(self.movie_cast_tab)
        action_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(action_frame, text="View Details", command=self.view_cast_details).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Edit Cast", command=self.edit_cast_dialog).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Remove Cast", command=self.remove_cast).pack(side='left', padx=5)
        
        # Load initial data
        self.load_all_movie_casts()
    
    def search_movies_for_cast(self, event=None):
        """Search movies by name and populate combobox"""
        search_term = self.cast_movie_entry.get().strip()
        if not search_term or len(search_term) < 2:
            self.cast_movie_combobox['values'] = []
            return
        
        try:
            with DbCtx() as (cur, conn):
                movies = admin_search_movies_by_title(cur, search_term, limit=20)
            
            # Format for combobox: "ID: Title (Year)"
            movie_options = []
            for movie in movies:
                year = movie['released_date'].year if movie.get('released_date') else 'Unknown'
                movie_options.append(f"{movie['movie_id']}: {movie['title']} ({year})")
            
            self.cast_movie_combobox['values'] = movie_options
            
        except Exception as e:
            # Silently fail for search-as-you-type
            pass
    
    def search_people_for_cast(self, event=None):
        """Search people by name and populate combobox"""
        search_term = self.cast_person_entry.get().strip()
        if not search_term or len(search_term) < 2:
            self.cast_person_combobox['values'] = []
            return
        
        try:
            with DbCtx() as (cur, conn):
                people = admin_search_people_by_name(cur, search_term, limit=20)
            
            # Format for combobox: "ID: Name"
            people_options = [f"{p['person_id']}: {p['name']}" for p in people]
            self.cast_person_combobox['values'] = people_options
            
        except Exception as e:
            # Silently fail for search-as-you-type
            pass
    
    def load_all_movie_casts(self):
        """Load all movie cast with movie and person names"""
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mc.movie_id, m.title, mc.person_id, p.name, 
                       mc.character, mc.cast_order, mc.credit_id
                FROM movie_cast mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                ORDER BY m.title, mc.cast_order NULLS LAST, p.name
                LIMIT 500
                """
                cur.execute(query)
                rows = cur.fetchall()
            
            self.update_cast_tree(rows)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load movie cast: {str(e)}")
    
    def update_cast_tree(self, rows):
        """Update treeview with cast data"""
        # Clear existing items
        for item in self.cast_tree.get_children():
            self.cast_tree.delete(item)
        
        # Add casts
        for row in rows:
            movie_id, movie_title, person_id, person_name, character, cast_order, credit_id = row
            
            # Format display values properly
            character_display = character if character else "N/A"
            cast_order_display = str(cast_order) if cast_order is not None else "N/A"
            credit_id_display = credit_id if credit_id else "N/A"
            
            self.cast_tree.insert('', 'end', values=(
                movie_id,
                movie_title,
                person_id,
                person_name,
                character_display,
                cast_order_display,
                credit_id_display
            ))
    
    def search_cast_by_movie(self):
        """Search cast by movie name"""
        selected_movie = self.cast_movie_combobox.get()
        if not selected_movie:
            messagebox.showwarning("Warning", "Please select a movie from the dropdown")
            return
        
        # Extract movie ID from combobox selection ("ID: Title (Year)")
        movie_id = int(selected_movie.split(':')[0])
        movie_title = selected_movie.split(':')[1].split('(')[0].strip()
        
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mc.movie_id, m.title, mc.person_id, p.name, 
                       mc.character, mc.cast_order, mc.credit_id
                FROM movie_cast mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                WHERE mc.movie_id = %s
                ORDER BY mc.cast_order NULLS LAST, p.name
                """
                cur.execute(query, (movie_id,))
                rows = cur.fetchall()
            
            self.update_cast_tree(rows)
            
            if not rows:
                messagebox.showinfo("No Results", f"Movie '{movie_title}' has no cast members")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def search_cast_by_person(self):
        """Search cast by person name"""
        selected_person = self.cast_person_combobox.get()
        if not selected_person:
            messagebox.showwarning("Warning", "Please select a person from the dropdown")
            return
        
        # Extract person ID from combobox selection ("ID: Name")
        person_id = int(selected_person.split(':')[0])
        person_name = selected_person.split(':')[1].strip()
        
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mc.movie_id, m.title, mc.person_id, p.name, 
                       mc.character, mc.cast_order, mc.credit_id
                FROM movie_cast mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                WHERE mc.person_id = %s
                ORDER BY m.title, mc.cast_order NULLS LAST
                """
                cur.execute(query, (person_id,))
                rows = cur.fetchall()
            
            self.update_cast_tree(rows)
            
            if not rows:
                messagebox.showinfo("No Results", f"Person '{person_name}' is not in any movies")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def search_cast_by_character(self):
        """Search cast by character name"""
        character_search = self.cast_character_entry.get().strip()
        if not character_search:
            messagebox.showwarning("Warning", "Please enter a character name to search")
            return
        
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mc.movie_id, m.title, mc.person_id, p.name, 
                       mc.character, mc.cast_order, mc.credit_id
                FROM movie_cast mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                WHERE mc.character ILIKE %s
                ORDER BY m.title, p.name
                """
                cur.execute(query, (f'%{character_search}%',))
                rows = cur.fetchall()
            
            self.update_cast_tree(rows)
            
            if not rows:
                messagebox.showinfo("No Results", f"No cast members found for character '{character_search}'")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def clear_cast_search(self):
        """Clear search fields and reload all cast"""
        self.cast_movie_entry.delete(0, tk.END)
        self.cast_person_entry.delete(0, tk.END)
        self.cast_character_entry.delete(0, tk.END)
        self.cast_movie_combobox.set('')
        self.cast_person_combobox.set('')
        self.cast_movie_combobox['values'] = []
        self.cast_person_combobox['values'] = []
        self.load_all_movie_casts()
    
    def view_cast_details(self):
        """Show detailed cast information"""
        selection = self.cast_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a cast member to view details")
            return
        
        values = self.cast_tree.item(selection[0])['values']
        movie_id, movie_title, person_id, person_name = values[0], values[1], values[2], values[3]
        
        try:
            with DbCtx() as (cur, conn):
                cast = admin_get_movie_cast(cur, movie_id, person_id)
            
            if cast:
                # Create details dialog
                details_window = tk.Toplevel(self.root)
                details_window.title(f"Cast Details: {person_name} in {movie_title}")
                details_window.geometry("500x350")
                details_window.transient(self.root)
                details_window.grab_set()
                
                # Details frame
                details_frame = ttk.LabelFrame(details_window, text="Cast Information", padding=15)
                details_frame.pack(fill='both', expand=True, padx=10, pady=10)
                
                # Cast info
                info_text = f"""Movie: {movie_title} (ID: {movie_id})
Person: {person_name} (ID: {person_id})
Character: {cast[2] or 'N/A'}
Cast Order: {cast[3] or 'N/A'}
Credit ID: {cast[4] or 'N/A'}"""
                                
                text_widget = tk.Text(details_frame, height=8, width=50, font=('Courier', 10))
                text_widget.insert('1.0', info_text)
                text_widget.config(state='disabled')
                text_widget.pack(pady=10, fill='both', expand=True)
                
                # Close button
                ttk.Button(details_frame, text="Close", 
                          command=details_window.destroy).pack(pady=10)
            else:
                messagebox.showerror("Error", "Cast member not found")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load cast details: {str(e)}")
    
    def add_cast_member_dialog(self):
        """Add a new cast member using names"""
        # Create dialog window
        dialog = tk.Toplevel(self.root)
        dialog.title("Add Cast Member")
        dialog.geometry("500x500")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Movie selection frame
        movie_frame = ttk.LabelFrame(dialog, text="Select Movie", padding=10)
        movie_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(movie_frame, text="Search Movie:").pack(anchor='w')
        movie_search_entry = ttk.Entry(movie_frame, width=50)
        movie_search_entry.pack(fill='x', pady=5)
        movie_search_entry.focus()
        
        ttk.Label(movie_frame, text="Select:").pack(anchor='w')
        movie_combobox = ttk.Combobox(movie_frame, width=50, state='readonly')
        movie_combobox.pack(fill='x', pady=5)
        
        # Person selection frame
        person_frame = ttk.LabelFrame(dialog, text="Select Person", padding=10)
        person_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(person_frame, text="Search Person:").pack(anchor='w')
        person_search_entry = ttk.Entry(person_frame, width=50)
        person_search_entry.pack(fill='x', pady=5)
        
        ttk.Label(person_frame, text="Select:").pack(anchor='w')
        person_combobox = ttk.Combobox(person_frame, width=50, state='readonly')
        person_combobox.pack(fill='x', pady=5)
        
        # Cast details frame
        details_frame = ttk.LabelFrame(dialog, text="Cast Details", padding=10)
        details_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(details_frame, text="Character:").grid(row=0, column=0, sticky='w', pady=5)
        character_entry = ttk.Entry(details_frame, width=40)
        character_entry.grid(row=0, column=1, sticky='w', pady=5, padx=5)
        
        ttk.Label(details_frame, text="Cast Order:").grid(row=1, column=0, sticky='w', pady=5)
        cast_order_entry = ttk.Entry(details_frame, width=40)
        cast_order_entry.grid(row=1, column=1, sticky='w', pady=5, padx=5)
        
        ttk.Label(details_frame, text="Credit ID:").grid(row=2, column=0, sticky='w', pady=5)
        credit_id_entry = ttk.Entry(details_frame, width=40)
        credit_id_entry.grid(row=2, column=1, sticky='w', pady=5, padx=5)
        
        # Buttons
        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(button_frame, text="Add Cast Member", 
                  command=lambda: self.add_cast_member_from_dialog(
                      movie_combobox.get(), 
                      person_combobox.get(),
                      character_entry.get().strip(),
                      cast_order_entry.get().strip(),
                      credit_id_entry.get().strip(),
                      dialog
                  )).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Cancel", 
                  command=dialog.destroy).pack(side='left', padx=5)
        
        # Bind search functionality
        def search_movies(event=None):
            search_term = movie_search_entry.get().strip()
            if len(search_term) >= 2:
                try:
                    with DbCtx() as (cur, conn):
                        movies = admin_search_movies_by_title(cur, search_term, limit=20)
                    options = [f"{m['movie_id']}: {m['title']}" for m in movies]
                    movie_combobox['values'] = options
                except:
                    pass
        
        def search_people(event=None):
            search_term = person_search_entry.get().strip()
            if len(search_term) >= 2:
                try:
                    with DbCtx() as (cur, conn):
                        people = admin_search_people_by_name(cur, search_term, limit=20)
                    options = [f"{p['person_id']}: {p['name']}" for p in people]
                    person_combobox['values'] = options
                except:
                    pass
        
        movie_search_entry.bind('<KeyRelease>', search_movies)
        person_search_entry.bind('<KeyRelease>', search_people)
    
    def add_cast_member_from_dialog(self, movie_selection, person_selection, character, cast_order, credit_id, dialog):
        """Add cast member from dialog selections"""
        if not movie_selection or not person_selection:
            messagebox.showwarning("Warning", "Please select both a movie and a person")
            return
        
        try:
            # Extract IDs from selections
            movie_id = int(movie_selection.split(':')[0])
            person_id = int(person_selection.split(':')[0])
            
            # Convert optional fields
            cast_order_int = int(cast_order) if cast_order.strip() else None
            character = character if character.strip() else None
            credit_id = credit_id if credit_id.strip() else None
            
            with DbCtx() as (cur, conn):
                # Check if association already exists
                existing = admin_get_movie_cast(cur, movie_id, person_id)
                if existing:
                    messagebox.showwarning("Warning", "This person is already cast in this movie")
                    return
                
                # Create cast member
                result = admin_create_movie_cast(cur, movie_id, person_id, character, cast_order_int, credit_id)
            
            if result:
                messagebox.showinfo("Success", "Cast member added successfully")
                dialog.destroy()
                self.load_all_movie_casts()
            else:
                messagebox.showerror("Error", "Failed to add cast member")
                
        except ValueError:
            messagebox.showerror("Input Error", "Cast Order must be a valid number")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to add cast member: {str(e)}")
    
    def edit_cast_dialog(self):
        """Edit selected cast member"""
        selection = self.cast_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a cast member to edit")
            return
        
        values = self.cast_tree.item(selection[0])['values']
        movie_id, movie_title, person_id, person_name = values[0], values[1], values[2], values[3]
        
        try:
            with DbCtx() as (cur, conn):
                cast = admin_get_movie_cast(cur, movie_id, person_id)
            
            if not cast:
                messagebox.showerror("Error", "Cast member not found")
                return
            
            # Create edit dialog
            dialog = tk.Toplevel(self.root)
            dialog.title(f"Edit Cast: {person_name} in {movie_title}")
            dialog.geometry("500x300")
            dialog.transient(self.root)
            dialog.grab_set()
            
            # Form frame
            form_frame = ttk.LabelFrame(dialog, text="Edit Cast Details", padding=15)
            form_frame.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Character
            ttk.Label(form_frame, text="Character:").grid(row=0, column=0, sticky='w', pady=5)
            character_entry = ttk.Entry(form_frame, width=40)
            character_entry.insert(0, cast[2] or "")
            character_entry.grid(row=0, column=1, sticky='w', pady=5, padx=5)
            character_entry.focus()
            
            # Cast Order
            ttk.Label(form_frame, text="Cast Order:").grid(row=1, column=0, sticky='w', pady=5)
            cast_order_entry = ttk.Entry(form_frame, width=40)
            cast_order_entry.insert(0, str(cast[3]) if cast[3] is not None else "")
            cast_order_entry.grid(row=1, column=1, sticky='w', pady=5, padx=5)
            
            # Credit ID
            ttk.Label(form_frame, text="Credit ID:").grid(row=2, column=0, sticky='w', pady=5)
            credit_id_entry = ttk.Entry(form_frame, width=40)
            credit_id_entry.insert(0, cast[4] or "")
            credit_id_entry.grid(row=2, column=1, sticky='w', pady=5, padx=5)
            
            # Buttons
            button_frame = ttk.Frame(dialog)
            button_frame.pack(fill='x', padx=10, pady=10)
            
            ttk.Button(button_frame, text="Save Changes", 
                      command=lambda: self.update_cast_from_dialog(
                          movie_id,
                          person_id,
                          character_entry.get().strip(),
                          cast_order_entry.get().strip(),
                          credit_id_entry.get().strip(),
                          dialog
                      )).pack(side='left', padx=5)
            ttk.Button(button_frame, text="Cancel", 
                      command=dialog.destroy).pack(side='left', padx=5)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load cast member for editing: {str(e)}")
    
    def update_cast_from_dialog(self, movie_id, person_id, character, cast_order, credit_id, dialog):
        """Update cast member from edit dialog"""
        try:
            update_data = {}
            
            # Handle character
            if character.strip():
                update_data['character'] = character
            else:
                update_data['character'] = None
            
            # Handle cast_order
            if cast_order.strip():
                update_data['cast_order'] = int(cast_order)
            else:
                update_data['cast_order'] = None
            
            # Handle credit_id
            if credit_id.strip():
                update_data['credit_id'] = credit_id
            else:
                update_data['credit_id'] = None
            
            with DbCtx() as (cur, conn):
                result = admin_update_movie_cast(cur, movie_id, person_id, update_data)
            
            if result:
                messagebox.showinfo("Success", "Cast member updated successfully")
                dialog.destroy()
                self.load_all_movie_casts()
            else:
                messagebox.showerror("Error", "Cast member not found")
                
        except ValueError:
            messagebox.showerror("Input Error", "Cast Order must be a valid number")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to update cast member: {str(e)}")
    
    def remove_cast(self):
        """Remove selected cast member"""
        selection = self.cast_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a cast member to remove")
            return
        
        values = self.cast_tree.item(selection[0])['values']
        movie_id, movie_title, person_id, person_name, character = values[0], values[1], values[2], values[3], values[4]
        
        if not messagebox.askyesno("Confirm Remove", 
                                 f"Are you sure you want to remove this cast member?\n\n"
                                 f"Movie: {movie_title}\n"
                                 f"Person: {person_name}\n"
                                 f"Character: {character}\n\n"
                                 "This action cannot be undone."):
            return
        
        try:
            with DbCtx() as (cur, conn):
                result = admin_delete_movie_cast(cur, movie_id, person_id)
            
            if result:
                messagebox.showinfo("Success", "Cast member removed successfully")
                self.load_all_movie_casts()
            else:
                messagebox.showerror("Error", "Cast member not found")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to remove cast member: {str(e)}")   

    #===========================================
    # Movie Crew Tab
    #===========================================
    def setup_movie_crew_tab(self):
        # Top frame with buttons
        top_frame = ttk.Frame(self.movie_crew_tab)
        top_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(top_frame, text="Load All Crew", command=self.load_all_movie_crews).pack(side='left', padx=5)
        ttk.Button(top_frame, text="Add Crew Member", command=self.add_crew_member_dialog).pack(side='left', padx=5)
        
        # Search frame
        search_frame = ttk.LabelFrame(self.movie_crew_tab, text="Search Crew", padding=10)
        search_frame.pack(fill='x', padx=10, pady=5)
        
        # Movie search
        ttk.Label(search_frame, text="Movie:").grid(row=0, column=0, padx=5, sticky='w')
        self.crew_movie_entry = ttk.Entry(search_frame, width=30)
        self.crew_movie_entry.grid(row=0, column=1, padx=5, sticky='w')
        self.crew_movie_entry.bind('<KeyRelease>', self.search_movies_for_crew)
        
        # Person search
        ttk.Label(search_frame, text="Person:").grid(row=1, column=0, padx=5, sticky='w')
        self.crew_person_entry = ttk.Entry(search_frame, width=30)
        self.crew_person_entry.grid(row=1, column=1, padx=5, sticky='w')
        self.crew_person_entry.bind('<KeyRelease>', self.search_people_for_crew)
        
        # Department search
        ttk.Label(search_frame, text="Department:").grid(row=2, column=0, padx=5, sticky='w')
        self.crew_department_entry = ttk.Entry(search_frame, width=30)
        self.crew_department_entry.grid(row=2, column=1, padx=5, sticky='w')
        
        # Job search
        ttk.Label(search_frame, text="Job:").grid(row=3, column=0, padx=5, sticky='w')
        self.crew_job_entry = ttk.Entry(search_frame, width=30)
        self.crew_job_entry.grid(row=3, column=1, padx=5, sticky='w')
        
        # Dropdowns for selection
        ttk.Label(search_frame, text="Select Movie:").grid(row=0, column=2, padx=5, sticky='w')
        self.crew_movie_combobox = ttk.Combobox(search_frame, width=30, state='readonly')
        self.crew_movie_combobox.grid(row=0, column=3, padx=5, sticky='w')
        
        ttk.Label(search_frame, text="Select Person:").grid(row=1, column=2, padx=5, sticky='w')
        self.crew_person_combobox = ttk.Combobox(search_frame, width=30, state='readonly')
        self.crew_person_combobox.grid(row=1, column=3, padx=5, sticky='w')
        
        # Buttons
        ttk.Button(search_frame, text="Search by Movie", command=self.search_crew_by_movie).grid(row=4, column=0, columnspan=2, pady=5)
        ttk.Button(search_frame, text="Search by Person", command=self.search_crew_by_person).grid(row=4, column=2, columnspan=2, pady=5)
        ttk.Button(search_frame, text="Search by Department", command=self.search_crew_by_department).grid(row=5, column=0, columnspan=2, pady=5)
        ttk.Button(search_frame, text="Search by Job", command=self.search_crew_by_job).grid(row=5, column=2, columnspan=2, pady=5)
        ttk.Button(search_frame, text="Clear", command=self.clear_crew_search).grid(row=6, column=1, columnspan=2, pady=5)
        
        # Results frame with treeview
        results_frame = ttk.LabelFrame(self.movie_crew_tab, text="Movie Crew", padding=10)
        results_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Treeview with scrollbar
        tree_scroll = ttk.Scrollbar(results_frame)
        tree_scroll.pack(side='right', fill='y')
        
        self.crew_tree = ttk.Treeview(results_frame, yscrollcommand=tree_scroll.set, 
                                    columns=('Movie ID', 'Movie Title', 'Person ID', 'Person Name', 'Department', 'Job', 'Credit ID'), 
                                    show='headings', height=15)
        
        # Configure columns
        self.crew_tree.heading('Movie ID', text='Movie ID')
        self.crew_tree.heading('Movie Title', text='Movie Title')
        self.crew_tree.heading('Person ID', text='Person ID')
        self.crew_tree.heading('Person Name', text='Person Name')
        self.crew_tree.heading('Department', text='Department')
        self.crew_tree.heading('Job', text='Job')
        self.crew_tree.heading('Credit ID', text='Credit ID')
        
        self.crew_tree.column('Movie ID', width=80)
        self.crew_tree.column('Movie Title', width=200)
        self.crew_tree.column('Person ID', width=80)
        self.crew_tree.column('Person Name', width=150)
        self.crew_tree.column('Department', width=120)
        self.crew_tree.column('Job', width=120)
        self.crew_tree.column('Credit ID', width=150)
        
        self.crew_tree.pack(fill='both', expand=True)
        tree_scroll.config(command=self.crew_tree.yview)
        
        # Double-click to view details
        self.crew_tree.bind('<Double-1>', lambda e: self.view_crew_details())
        
        # Action buttons
        action_frame = ttk.Frame(self.movie_crew_tab)
        action_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(action_frame, text="View Details", command=self.view_crew_details).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Edit Crew", command=self.edit_crew_dialog).pack(side='left', padx=5)
        ttk.Button(action_frame, text="Remove Crew", command=self.remove_crew).pack(side='left', padx=5)
        
        # Load initial data
        self.load_all_movie_crews()
    
    def search_movies_for_crew(self, event=None):
        """Search movies by name and populate combobox"""
        search_term = self.crew_movie_entry.get().strip()
        if not search_term or len(search_term) < 2:
            self.crew_movie_combobox['values'] = []
            return
        
        try:
            with DbCtx() as (cur, conn):
                movies = admin_search_movies_by_title(cur, search_term, limit=20)
            
            # Format for combobox: "ID: Title (Year)"
            movie_options = []
            for movie in movies:
                year = movie['released_date'].year if movie.get('released_date') else 'Unknown'
                movie_options.append(f"{movie['movie_id']}: {movie['title']} ({year})")
            
            self.crew_movie_combobox['values'] = movie_options
            
        except Exception as e:
            # Silently fail for search-as-you-type
            pass
    
    def search_people_for_crew(self, event=None):
        """Search people by name and populate combobox"""
        search_term = self.crew_person_entry.get().strip()
        if not search_term or len(search_term) < 2:
            self.crew_person_combobox['values'] = []
            return
        
        try:
            with DbCtx() as (cur, conn):
                people = admin_search_people_by_name(cur, search_term, limit=20)
            
            # Format for combobox: "ID: Name"
            people_options = [f"{p['person_id']}: {p['name']}" for p in people]
            self.crew_person_combobox['values'] = people_options
            
        except Exception as e:
            # Silently fail for search-as-you-type
            pass
    
    def load_all_movie_crews(self):
        """Load all movie crew with movie and person names"""
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mc.movie_id, m.title, mc.person_id, p.name, 
                    mc.department, mc.job, mc.credit_id
                FROM movie_crew mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                ORDER BY m.title, mc.department, mc.job, p.name
                LIMIT 500
                """
                cur.execute(query)
                rows = cur.fetchall()
            
            self.update_crew_tree(rows)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load movie crew: {str(e)}")
    
    def update_crew_tree(self, rows):
        """Update treeview with crew data"""
        # Clear existing items
        for item in self.crew_tree.get_children():
            self.crew_tree.delete(item)
        
        # Add crew
        for row in rows:
            movie_id, movie_title, person_id, person_name, department, job, credit_id = row
            
            # Format display values properly
            credit_id_display = credit_id if credit_id else "N/A"
            
            self.crew_tree.insert('', 'end', values=(
                movie_id,
                movie_title,
                person_id,
                person_name,
                department,
                job,
                credit_id_display
            ))
    
    def search_crew_by_movie(self):
        """Search crew by movie name"""
        selected_movie = self.crew_movie_combobox.get()
        if not selected_movie:
            messagebox.showwarning("Warning", "Please select a movie from the dropdown")
            return
        
        # Extract movie ID from combobox selection ("ID: Title (Year)")
        movie_id = int(selected_movie.split(':')[0])
        movie_title = selected_movie.split(':')[1].split('(')[0].strip()
        
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mc.movie_id, m.title, mc.person_id, p.name, 
                    mc.department, mc.job, mc.credit_id
                FROM movie_crew mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                WHERE mc.movie_id = %s
                ORDER BY mc.department, mc.job, p.name
                """
                cur.execute(query, (movie_id,))
                rows = cur.fetchall()
            
            self.update_crew_tree(rows)
            
            if not rows:
                messagebox.showinfo("No Results", f"Movie '{movie_title}' has no crew members")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def search_crew_by_person(self):
        """Search crew by person name"""
        selected_person = self.crew_person_combobox.get()
        if not selected_person:
            messagebox.showwarning("Warning", "Please select a person from the dropdown")
            return
        
        # Extract person ID from combobox selection ("ID: Name")
        person_id = int(selected_person.split(':')[0])
        person_name = selected_person.split(':')[1].strip()
        
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mc.movie_id, m.title, mc.person_id, p.name, 
                    mc.department, mc.job, mc.credit_id
                FROM movie_crew mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                WHERE mc.person_id = %s
                ORDER BY m.title, mc.department, mc.job
                """
                cur.execute(query, (person_id,))
                rows = cur.fetchall()
            
            self.update_crew_tree(rows)
            
            if not rows:
                messagebox.showinfo("No Results", f"Person '{person_name}' is not crew on any movies")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def search_crew_by_department(self):
        """Search crew by department"""
        department_search = self.crew_department_entry.get().strip()
        if not department_search:
            messagebox.showwarning("Warning", "Please enter a department to search")
            return
        
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mc.movie_id, m.title, mc.person_id, p.name, 
                    mc.department, mc.job, mc.credit_id
                FROM movie_crew mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                WHERE mc.department ILIKE %s
                ORDER BY m.title, mc.job, p.name
                """
                cur.execute(query, (f'%{department_search}%',))
                rows = cur.fetchall()
            
            self.update_crew_tree(rows)
            
            if not rows:
                messagebox.showinfo("No Results", f"No crew found in department '{department_search}'")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def search_crew_by_job(self):
        """Search crew by job title"""
        job_search = self.crew_job_entry.get().strip()
        if not job_search:
            messagebox.showwarning("Warning", "Please enter a job title to search")
            return
        
        try:
            with DbCtx() as (cur, conn):
                query = """
                SELECT mc.movie_id, m.title, mc.person_id, p.name, 
                    mc.department, mc.job, mc.credit_id
                FROM movie_crew mc
                JOIN movies m ON mc.movie_id = m.movie_id
                JOIN people p ON mc.person_id = p.person_id
                WHERE mc.job ILIKE %s
                ORDER BY m.title, mc.department, p.name
                """
                cur.execute(query, (f'%{job_search}%',))
                rows = cur.fetchall()
            
            self.update_crew_tree(rows)
            
            if not rows:
                messagebox.showinfo("No Results", f"No crew found with job '{job_search}'")
                
        except Exception as e:
            messagebox.showerror("Error", f"Search failed: {str(e)}")
    
    def clear_crew_search(self):
        """Clear search fields and reload all crew"""
        self.crew_movie_entry.delete(0, tk.END)
        self.crew_person_entry.delete(0, tk.END)
        self.crew_department_entry.delete(0, tk.END)
        self.crew_job_entry.delete(0, tk.END)
        self.crew_movie_combobox.set('')
        self.crew_person_combobox.set('')
        self.crew_movie_combobox['values'] = []
        self.crew_person_combobox['values'] = []
        self.load_all_movie_crews()
    
    def view_crew_details(self):
        """Show detailed crew information"""
        selection = self.crew_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a crew member to view details")
            return
        
        values = self.crew_tree.item(selection[0])['values']
        movie_id, movie_title, person_id, person_name = values[0], values[1], values[2], values[3]
        department, job = values[4], values[5]
        
        try:
            with DbCtx() as (cur, conn):
                crew = admin_get_movie_crew(cur, movie_id, person_id, job)
            
            if crew:
                # Create details dialog
                details_window = tk.Toplevel(self.root)
                details_window.title(f"Crew Details: {person_name} - {job}")
                details_window.geometry("500x350")
                details_window.transient(self.root)
                details_window.grab_set()
                
                # Details frame
                details_frame = ttk.LabelFrame(details_window, text="Crew Information", padding=15)
                details_frame.pack(fill='both', expand=True, padx=10, pady=10)
                
                # Crew info
                info_text = f"""Movie: {movie_title} (ID: {movie_id})
                Person: {person_name} (ID: {person_id})
                Department: {department}
                Job: {job}
                Credit ID: {crew[4] or 'N/A'}"""
                                
                text_widget = tk.Text(details_frame, height=8, width=50, font=('Courier', 10))
                text_widget.insert('1.0', info_text)
                text_widget.config(state='disabled')
                text_widget.pack(pady=10, fill='both', expand=True)
                
                # Close button
                ttk.Button(details_frame, text="Close", 
                        command=details_window.destroy).pack(pady=10)
            else:
                messagebox.showerror("Error", "Crew member not found")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load crew details: {str(e)}")
    
    def add_crew_member_dialog(self):
        """Add a new crew member using names"""
        # Create dialog window
        dialog = tk.Toplevel(self.root)
        dialog.title("Add Crew Member")
        dialog.geometry("500x550")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Movie selection frame
        movie_frame = ttk.LabelFrame(dialog, text="Select Movie", padding=10)
        movie_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(movie_frame, text="Search Movie:").pack(anchor='w')
        movie_search_entry = ttk.Entry(movie_frame, width=50)
        movie_search_entry.pack(fill='x', pady=5)
        movie_search_entry.focus()
        
        ttk.Label(movie_frame, text="Select:").pack(anchor='w')
        movie_combobox = ttk.Combobox(movie_frame, width=50, state='readonly')
        movie_combobox.pack(fill='x', pady=5)
        
        # Person selection frame
        person_frame = ttk.LabelFrame(dialog, text="Select Person", padding=10)
        person_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(person_frame, text="Search Person:").pack(anchor='w')
        person_search_entry = ttk.Entry(person_frame, width=50)
        person_search_entry.pack(fill='x', pady=5)
        
        ttk.Label(person_frame, text="Select:").pack(anchor='w')
        person_combobox = ttk.Combobox(person_frame, width=50, state='readonly')
        person_combobox.pack(fill='x', pady=5)
        
        # Crew details frame
        details_frame = ttk.LabelFrame(dialog, text="Crew Details", padding=10)
        details_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(details_frame, text="Department *:").grid(row=0, column=0, sticky='w', pady=5)
        department_entry = ttk.Entry(details_frame, width=40)
        department_entry.grid(row=0, column=1, sticky='w', pady=5, padx=5)
        
        ttk.Label(details_frame, text="Job *:").grid(row=1, column=0, sticky='w', pady=5)
        job_entry = ttk.Entry(details_frame, width=40)
        job_entry.grid(row=1, column=1, sticky='w', pady=5, padx=5)
        
        ttk.Label(details_frame, text="Credit ID:").grid(row=2, column=0, sticky='w', pady=5)
        credit_id_entry = ttk.Entry(details_frame, width=40)
        credit_id_entry.grid(row=2, column=1, sticky='w', pady=5, padx=5)
        
        # Buttons
        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(button_frame, text="Add Crew Member", 
                command=lambda: self.add_crew_member_from_dialog(
                    movie_combobox.get(), 
                    person_combobox.get(),
                    department_entry.get().strip(),
                    job_entry.get().strip(),
                    credit_id_entry.get().strip(),
                    dialog
                )).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Cancel", 
                command=dialog.destroy).pack(side='left', padx=5)
        
        # Bind search functionality
        def search_movies(event=None):
            search_term = movie_search_entry.get().strip()
            if len(search_term) >= 2:
                try:
                    with DbCtx() as (cur, conn):
                        movies = admin_search_movies_by_title(cur, search_term, limit=20)
                    options = [f"{m['movie_id']}: {m['title']}" for m in movies]
                    movie_combobox['values'] = options
                except:
                    pass
        
        def search_people(event=None):
            search_term = person_search_entry.get().strip()
            if len(search_term) >= 2:
                try:
                    with DbCtx() as (cur, conn):
                        people = admin_search_people_by_name(cur, search_term, limit=20)
                    options = [f"{p['person_id']}: {p['name']}" for p in people]
                    person_combobox['values'] = options
                except:
                    pass
        
        movie_search_entry.bind('<KeyRelease>', search_movies)
        person_search_entry.bind('<KeyRelease>', search_people)
    
    def add_crew_member_from_dialog(self, movie_selection, person_selection, department, job, credit_id, dialog):
        """Add crew member from dialog selections"""
        if not movie_selection or not person_selection:
            messagebox.showwarning("Warning", "Please select both a movie and a person")
            return
        
        if not department or not job:
            messagebox.showwarning("Warning", "Department and Job are required")
            return
        
        try:
            # Extract IDs from selections
            movie_id = int(movie_selection.split(':')[0])
            person_id = int(person_selection.split(':')[0])
            
            # Convert optional field
            credit_id = credit_id if credit_id.strip() else None
            
            with DbCtx() as (cur, conn):
                # Check if association already exists
                existing = admin_get_movie_crew(cur, movie_id, person_id, job)
                if existing:
                    messagebox.showwarning("Warning", "This person already has this job on this movie")
                    return
                
                # Create crew member
                result = admin_create_movie_crew(cur, movie_id, person_id, department, job, credit_id)
            
            if result:
                messagebox.showinfo("Success", "Crew member added successfully")
                dialog.destroy()
                self.load_all_movie_crews()
            else:
                messagebox.showerror("Error", "Failed to add crew member")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to add crew member: {str(e)}")
    
    def edit_crew_dialog(self):
        """Edit selected crew member"""
        selection = self.crew_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a crew member to edit")
            return
        
        values = self.crew_tree.item(selection[0])['values']
        movie_id, movie_title, person_id, person_name, department, job = values[0], values[1], values[2], values[3], values[4], values[5]
        
        try:
            with DbCtx() as (cur, conn):
                crew = admin_get_movie_crew(cur, movie_id, person_id, job)
            
            if not crew:
                messagebox.showerror("Error", "Crew member not found")
                return
            
            # Create edit dialog
            dialog = tk.Toplevel(self.root)
            dialog.title(f"Edit Crew: {person_name} - {job}")
            dialog.geometry("500x350")
            dialog.transient(self.root)
            dialog.grab_set()
            
            # Form frame
            form_frame = ttk.LabelFrame(dialog, text="Edit Crew Details", padding=15)
            form_frame.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Department
            ttk.Label(form_frame, text="Department *:").grid(row=0, column=0, sticky='w', pady=5)
            department_entry = ttk.Entry(form_frame, width=40)
            department_entry.insert(0, crew[2])  # department
            department_entry.grid(row=0, column=1, sticky='w', pady=5, padx=5)
            department_entry.focus()
            
            # Job
            ttk.Label(form_frame, text="Job *:").grid(row=1, column=0, sticky='w', pady=5)
            job_entry = ttk.Entry(form_frame, width=40)
            job_entry.insert(0, crew[3])  # job
            job_entry.grid(row=1, column=1, sticky='w', pady=5, padx=5)
            
            # Credit ID
            ttk.Label(form_frame, text="Credit ID:").grid(row=2, column=0, sticky='w', pady=5)
            credit_id_entry = ttk.Entry(form_frame, width=40)
            credit_id_entry.insert(0, crew[4] or "")  # credit_id
            credit_id_entry.grid(row=2, column=1, sticky='w', pady=5, padx=5)
            
            # Buttons
            button_frame = ttk.Frame(dialog)
            button_frame.pack(fill='x', padx=10, pady=10)
            
            ttk.Button(button_frame, text="Save Changes", 
                    command=lambda: self.update_crew_from_dialog(
                        movie_id,
                        person_id,
                        job,  # original job for identification
                        department_entry.get().strip(),
                        job_entry.get().strip(),
                        credit_id_entry.get().strip(),
                        dialog
                    )).pack(side='left', padx=5)
            ttk.Button(button_frame, text="Cancel", 
                    command=dialog.destroy).pack(side='left', padx=5)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load crew member for editing: {str(e)}")
    
    def update_crew_from_dialog(self, movie_id, person_id, original_job, department, job, credit_id, dialog):
        """Update crew member from edit dialog"""
        if not department or not job:
            messagebox.showwarning("Warning", "Department and Job are required")
            return
        
        try:
            update_data = {
                'department': department,
                'job': job
            }
            
            # Handle credit_id
            if credit_id.strip():
                update_data['credit_id'] = credit_id
            else:
                update_data['credit_id'] = None
            
            with DbCtx() as (cur, conn):
                result = admin_update_movie_crew(cur, movie_id, person_id, original_job, update_data)
            
            if result:
                messagebox.showinfo("Success", "Crew member updated successfully")
                dialog.destroy()
                self.load_all_movie_crews()
            else:
                messagebox.showerror("Error", "Crew member not found")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to update crew member: {str(e)}")
    
    def remove_crew(self):
        """Remove selected crew member"""
        selection = self.crew_tree.selection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a crew member to remove")
            return
        
        values = self.crew_tree.item(selection[0])['values']
        movie_id, movie_title, person_id, person_name, department, job = values[0], values[1], values[2], values[3], values[4], values[5]
        
        if not messagebox.askyesno("Confirm Remove", 
                                f"Are you sure you want to remove this crew member?\n\n"
                                f"Movie: {movie_title}\n"
                                f"Person: {person_name}\n"
                                f"Department: {department}\n"
                                f"Job: {job}\n\n"
                                "This action cannot be undone."):
            return
        
        try:
            with DbCtx() as (cur, conn):
                result = admin_delete_movie_crew(cur, movie_id, person_id, job)
            
            if result:
                messagebox.showinfo("Success", "Crew member removed successfully")
                self.load_all_movie_crews()
            else:
                messagebox.showerror("Error", "Crew member not found")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to remove crew member: {str(e)}")

# ----------------------------------------------------
# Run
# ----------------------------------------------------
if __name__ == "__main__":
    root = tk.Tk()
    app = MovieAdminApp(root)
    root.mainloop()