# Cinépolis Chile Movie Scraper

[![Python Version](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/downloads/)
[![BeautifulSoup](https://img.shields.io/badge/beautifulsoup-4.12.2-green.svg)](https://www.crummy.com/software/BeautifulSoup/)
[![Requests](https://img.shields.io/badge/requests-2.31.0-red.svg)](https://requests.readthedocs.io/)

A Python tool for scraping movie information from Cinépolis Chile's website. This script automatically downloads and extracts structured movie data, saving it in an organized folder structure.

## 📋 Features

- Downloads the main page from Cinépolis Chile
- Extracts all available movie URLs
- Processes each movie page to extract relevant information
- Creates a structured folder system for each movie
- Generates three files per movie:
  - `description.txt`: Complete text content from the movie's page
  - `webpage.txt`: The exact URL of the movie page
  - `texto_estructurado.txt`: Formatted structured data with key movie information
- Handles special characters and ensures valid file names
- Automatically updates existing movie information when re-run

## 🚀 Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/movies_info.git
cd movies_info
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## 💻 Usage

### Step 1: Run the Web Scraper

```bash
python wget.py
```

The script will:
1. Download the Cinépolis Chile main page
2. Extract all movie URLs  
3. Process each movie page
4. **REPLACE** existing sample data in `/movies` directory with fresh scraped content

### Step 2: Verify Output Structure

After execution, your `/movies` directory will contain:
- Individual movie folders with structured metadata
- Each folder contains: `description.txt`, `webpage.txt`, `texto_estructurado.txt`
- **REPLACES** the sample movie data provided in the repository

> 📁 **Sample Movie Data Available**: Pre-scraped movie metadata examples can be found in the `/movies` directory. A complete dataset can be accessed via [Google Drive](https://drive.google.com/drive/folders/1FzNKnWKWpBXWzYgMD24FJCjkY8wIcwtW?usp=drive_link) for reference and testing purposes.

### Output Structure

The script creates a folder structure as follows:

```
movies/
  ├── MOVIE-NAME-1/
  │   ├── description.txt     # Complete text from the page
  │   ├── webpage.txt         # URL of the movie page
  │   └── texto_estructurado.txt  # Structured information
  ├── MOVIE-NAME-2/
  │   ├── ...
  └── ...
```

### Structured Information Format

The `texto_estructurado.txt` file contains the following information in a structured format:

```
1 nombre pelicula: [MOVIE TITLE]
2 Restriccion Edad: [AGE RESTRICTION]
3 duracion: [DURATION] min
4 categoria: [GENRE]
5 Sinopsis: [PLOT SUMMARY]
6 Actores: [ACTORS LIST]
7 Directores: [DIRECTORS LIST]
```

## 🔧 Technical Details

The scraper uses:
- **requests**: For downloading web pages
- **BeautifulSoup4**: For HTML parsing and data extraction
- **Regular expressions**: For extracting structured information

The script handles various edge cases:
- Different page structures
- Special characters in movie titles
- Missing information fields
- Error handling for network failures

## ⚠️ Disclaimer

This tool is for educational purposes only. Please respect Cinépolis Chile's terms of service and robots.txt directives. Consider implementing rate limiting to avoid overwhelming their servers.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

