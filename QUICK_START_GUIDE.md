# Quick Start Guide - MongoDB Database Switching

## 🚀 You're All Set!

Your MongoDB integration is **complete and ready to use**! Here's how to use it:

## Step-by-Step Guide

### 1. Start the Application

Open your terminal and run:
```bash
cd "c:\Users\LIM\OneDrive\Documents\Database Files"
python app.py
```

You should see:
```
* Running on http://127.0.0.1:5000
```

### 2. Open Your Browser

Navigate to: **http://localhost:5000**

### 3. Switch to MongoDB

Look at the **top navigation bar** (screenshot reference from your image):

```
┌────────────────────────────────────────────────────────┐
│  Movie DB Admin  [Movies] [Add Movie] ... [PostgreSQL ▼] │
└────────────────────────────────────────────────────────┘
```

1. **Click** on the database dropdown (shows `PostgreSQL` in a badge)
2. **Select** the `MongoDB` radio button
3. The page will **automatically reload**

### 4. Verify MongoDB Data

After switching to MongoDB:

**You should see:**
- ✓ Badge changes to `MongoDB`
- ✓ Movies list updates with MongoDB data
- ✓ 45,463 movies available
- ✓ Search works with MongoDB

**Test it:**
1. Click on **Movies** tab
2. Search for "Toy Story"
3. Click on "Toy Story" to view details
4. You'll see genres embedded in the movie data

## What's Different with MongoDB?

### When Using PostgreSQL (Original):
- Full CRUD operations (Create, Read, Update, Delete)
- All tabs functional (Genres, Companies, People, etc.)
- Relational data with joins

### When Using MongoDB (New):
- **Movies Tab**: ✓ Read-only access (List + View + Search)
- **Genres**: Embedded in movie documents
- **Other Tabs**: Still use PostgreSQL (Companies, People, etc.)

## Current Database Indicator

The navigation bar always shows which database you're using:

- **`PostgreSQL`** badge = Using relational database
- **`MongoDB`** badge = Using document database

## Data Verification

### Check MongoDB has data:
```bash
python test_mongo_setup.py
```

Expected output:
```
Total movies in database: 45463
✓ Found 45463 movies
✓ All tests passed!
```

### If you need to re-import data:
```bash
python import_mongo_data.py
```

## Features Available with MongoDB

### ✓ Working Features:

1. **List Movies**
   - Shows up to 100 movies at a time
   - Sorted by ID (newest first)

2. **Search Movies**
   - Case-insensitive search
   - Search by title
   - Search by movie ID

3. **View Movie Details**
   - All movie information
   - Embedded genres
   - Poster images
   - Overview and metadata

### ⚠ Not Yet Implemented:

- Create new movies (MongoDB)
- Edit existing movies (MongoDB)
- Delete movies (MongoDB)
- Other tabs (still use PostgreSQL)

## Switching Back to PostgreSQL

At any time:
1. Click the **database dropdown**
2. Select **`PostgreSQL`**
3. Page reloads with PostgreSQL data

## Tips & Tricks

### Quick Toggle
You can switch databases as often as you want. Each time you switch:
- Session updates
- Page reloads
- Data source changes

### Mixed Usage
You can:
- View movies in MongoDB
- Switch to PostgreSQL to edit them
- Switch back to MongoDB to view
- All data is separate but movie IDs match

### Search Comparison
Try searching for the same movie in both databases:
1. Search "Toy Story" in PostgreSQL
2. Switch to MongoDB
3. Search "Toy Story" again
4. Compare results

## MongoDB vs PostgreSQL - Quick Reference

| Feature | PostgreSQL | MongoDB |
|---------|-----------|---------|
| Movies List | ✓ | ✓ |
| Movie Search | ✓ | ✓ |
| Movie View | ✓ | ✓ |
| Movie Create | ✓ | ✗ |
| Movie Edit | ✓ | ✗ |
| Movie Delete | ✓ | ✗ |
| Genres (separate) | ✓ | ✗ |
| Genres (embedded) | ✗ | ✓ |
| Companies | ✓ | ✗ |
| People | ✓ | ✗ |
| Ratings | ✓ | ✗ |

## Example Workflow

### Scenario: Browse movies in both databases

**Step 1:** Start with PostgreSQL
- View movies list
- Search for "Matrix"
- Note the results

**Step 2:** Switch to MongoDB
- Click database dropdown
- Select MongoDB
- Search for "Matrix" again
- Compare with PostgreSQL results

**Step 3:** View Movie Details
- Click on "The Matrix"
- See embedded genres
- Note the poster and overview

**Step 4:** Switch back
- Click database dropdown
- Select PostgreSQL
- You're back to relational data

## Understanding the Data

### PostgreSQL Structure:
```sql
movies table
├─ movie_id (PK)
├─ title
├─ overview
└─ ...

movie_genres table (junction)
├─ movie_id (FK) ──┐
└─ genre_id (FK)   │
                   │
genres table       │
├─ genre_id (PK) ←─┘
└─ genre_name
```

### MongoDB Structure:
```javascript
movies collection
{
  id: 862,
  original_title: "Toy Story",
  genres: [              // ← Embedded!
    { id: 16, name: "Animation" },
    { id: 35, name: "Comedy" }
  ],
  overview: "...",
  ...
}
```

## Troubleshooting

### Problem: Database dropdown not showing
**Solution:** Refresh the page (F5)

### Problem: MongoDB shows no movies
**Solution:**
```bash
python import_mongo_data.py
python test_mongo_setup.py
```

### Problem: Switch doesn't work
**Solution:**
- Check browser console for errors
- Clear browser cache
- Restart Flask app

### Problem: MongoDB connection error
**Solution:**
- Ensure MongoDB is running
- Check MongoDB is listening on port 27017
- Run: `mongo --version` to verify installation

## Need Help?

1. **Setup Issues:** See `MONGODB_SETUP.md`
2. **Implementation Details:** See `IMPLEMENTATION_SUMMARY.md`
3. **Test MongoDB:** Run `python test_mongo_setup.py`

## Success Checklist ✓

- [ ] Flask app running
- [ ] Browser open at localhost:5000
- [ ] Can see database dropdown
- [ ] Can switch to MongoDB
- [ ] Badge changes to "MongoDB"
- [ ] Movies list shows data
- [ ] Search works
- [ ] Movie details display
- [ ] Can switch back to PostgreSQL

If all checked, **you're successfully using the MongoDB integration!** 🎉

---

**Pro Tip:** Keep the terminal open where Flask is running to see real-time logs of database queries and errors.
