# DuckDB Analysis Environment for Book Metadata

> **Note:** The default configuration is optimized for systems with 30GB RAM. If you have more resources available, you can adjust the parameters accordingly, especially the chunk size (currently set to 10MB) for file processing.

## 🌟 Features

- **Data Processing Pipeline**
  - GZ file decompression
  - JSON to Parquet conversion
  - Chunked processing for large datasets (10MB chunks by default)
  - Memory-efficient operations

- **Analysis Capabilities**
  - Advanced SQL queries with DuckDB
  - Interactive data exploration
  - Rich visualizations with Matplotlib and Seaborn
  - Statistical analysis tools

- **Technical Stack**
  - DuckDB for high-performance SQL queries
  - Jupyter for interactive analysis
  - Pandas for data manipulation
  - Matplotlib & Seaborn for visualization
  - Docker for environment consistency
  
- **Smart Schema Analysis**
   - Automated JSON structure analysis:
   - Samples 10% of dataset for efficient schema detection (configurable)
   - Intelligent column creation based on key frequency (>50% threshold, adjustable)
   - Optimized memory usage through chunked processing
   - Dynamic schema evolution handling

## 📥 Data Setup

### System Requirements
- Minimum: 30GB RAM (default configuration)
- Storage: Depends on dataset size
- Adjustable parameters:
  - Chunk size: Default 10MB (can be increased with more RAM)
  - DuckDB memory limit: Default 28GB (can be adjusted in configuration)

### File Structure
```
project/
├── data/
│   ├── elasticsearch/    # Original .gz files from Elasticsearch
│   ├── elasticsearchF/   # Final .parquet files from Elasticsearch
│   ├── elasticsearchAux/ # Additional Elasticsearch data
│   ├── elasticsearchAuxF/# Final .parquet files from Elasticsearchaux
│   ├── aac/             # AAC Data .zst files
│   ├── aacF/            # Final .parquet files from aac data
│   ├── mariadb/         # Original MariaDB data files
│   ├── mariadbF/        # Final .parquet files from MariaDB data
├── notebooks/
│   ├── Elasticsearch_Queries.ipynb    # Elasticsearch analysis notebook
│   ├── ElasticsearchAux_Queries.ipynb # ElasticsearchAux analysis notebook
│   ├── AAC_Queries.ipynb             # AAC analysis notebooks
│   └── Mariadb_Queries.ipynb         # MariaDB analysis notebook
```

### Data Processing Steps

1. **Elasticsearch Data**
   - Place all `.gz` files in the `data/elasticsearch/` folder and all `.gz` files in the `data/elasticsearchAux/` folder.
2. **AAC Data**
   - Place all `.zst` files in the `data/aac/` folder.
   - Then, run the processing script to convert the files to Parquet format.



### Processing the data 

#### Using initialScript.py
```bash
python initialScript.py
```

This script will:
1. Create all necessary directories
2. Process Elasticsearch, ElasticsearchAux and AAC data
3. Convert to Parquet format


## 📊 Elasticsearch Analysis Examples

### Publication Year Distribution
![Publication Year Distribution](images/Publication%20Year%20Distribution.png)

Analysis of publication years across the Elasticsearch dataset.

### Language Analysis
![Language Distribution](images/Language%20Distribution.png)

Distribution of languages in the Elasticsearch collection.

### Rare vs. Non-Rare Books Analysis
![Rare vs. Non-Rare](images/Rare%20vs.%20Non-Rare.png)

Distribution between rare and non-rare books in the Elasticsearch dataset.

### AACID Analysis
![AACID Analysis](images/AACID%20Analysis.png)

AACID patterns and distribution analysis.

### Download Availability Analysis
![Download Availability](images/Download%20Availability.png)

Analysis of download options in the Elasticsearch dataset.

### Cover URL Analysis
![Cover URL Analysis](images/Cover%20URL%20Analysis.png)

Distribution of cover image availability.

### File Size Distribution by Content Type
![File Size Distribution](images/File%20Size%20Distribution.png)

File size analysis across different content types.

### Classification Analysis
![Classification Analysis](images/Classification%20Analysis.png)

Book classification distribution analysis.

### Score Analysis
![Score Analysis](images/Score%20Analysis.png)

Analysis of base rank scores in the dataset.

## 📊 ElasticsearchAux Analysis Examples (These data are not complete, they are the result of the query in 1/12 of the dataset.)

### Content Types Analysis
![Content Types Analysis](images/1Content%20Types%20Analysis.png)
Distribution of different content types across the dataset.

### Publication Year Analysis
![Publication Year Analysis](images/2Publication%20Year%20Analysis.png)
Histogram showing publication trends over different years.

### Language Distribution Analysis
![Language Distribution Analysis](images/3Language%20Distribution%20Analysis.png)
Breakdown of languages represented in the dataset.

### File Size Analysis
![File Size Analysis](images/4File%20Size%20Analysis.png)
Distribution of file sizes throughout the collection.

### Publisher Analysis
![Publisher Analysis](images/5Publisher%20Analysis.png)
Top publishers represented in the dataset by volume.

### File Extensions Analysis
![File Extensions Analysis](images/6File%20Extensions%20Analysis.png)
Frequency of different file formats in the collection.

### Access Type Analysis
![Access Type Analysis](images/7Access%20Type%20Analysis.png)
Analysis of different access methods for the content.

### Publications Over Time by Content Type
![Publications Over Time by Content Type](images/8Publications%20Over%20Time%20by%20Content%20Type.png)
Trend analysis of how content types evolved over publication years.

### Author Analysis
![Author Analysis](images/9Author%20Analysis.png)
Most represented authors in the dataset.

### Language and Publication Year Correlation
![Language and Publication Year Correlation](images/10Language%20and%20Publication%20Year%20Correlation.png)
How language distribution changes across publication years.

### Torrent Availability Analysis
![Torrent Availability Analysis](images/12Torrent%20Availability%20Analysis.png)
Analysis of torrent availability for the content.

### Title Word Cloud Analysis
![Title Word Cloud Analysis](images/14Title%20Word%20Cloud%20Analysis.png)
Visual representation of most common words in titles.

### Publication Year and File Size Correlation
![Publication Year and File Size Correlation](images/15Publication%20Year%20and%20File%20Size%20Correlation.png)
Examining how file sizes relate to publication years.

### File Extension and Content Type Analysis
![File Extension and Content Type Analysis](images/19File%20Extension%20and%20Content%20Type%20Analysis.png)
Relationship between file formats and content categories.

### Combined Dataset Comparison Overview
![Combined Dataset Comparison Overview](images/28Combined%20Dataset%20Comparison%20Overview.png)
Comparative analysis of multiple datasets in the collection.

## 📊 AAC Analysis Examples

### Airiti Books Analysis
![Airiti Books Analysis](images/Airiti%20Books%20Analysis.png)

### Bloomsbury Analysis
![Publication Year Analysis](images/ISBNAnalysisandCollectionTypes_BloomsburyAnalysis.png)
ISBN Analysis and Collection Types

### CERLALC Records Analysis
![CERLALC Records Analysis Top subjects and publishers](images/TopsubjectsandpublishersCERLALCRecordsAnalysis.png)
Top subjects and publishers

### Chinese Architecture Analysis
![Publication Analysis by Year and Publisher](images/PublicationAnalysisbyYearandPublisherChinese.png)
Publication Analysis by Year and Publisher

### Czech Records Analysis
![Czech Records Analysis](images/KeywordAnalysisCzech.png)
Keyword Analysis

### DuXiu Records Analysis
![SQL query to get the number of records by ISBN-10](images/DuXiuRecordsAnalysis.png)
SQL query to get the number of records by ISBN-10

### EBSCO Records Analysis
![EBSCO Records Analysis](images/EBSCORecordsAnalysis.png)
SQL query to analyze the subjects and subject types

### Google Books Analysis
![Google Books Analysis Language](images/GoogleBooksAnalysisLanguage.png)
SQL query to analyze books by language, keeping top 10 languages and grouping others as "Other"

### Goodreads Analysis
![SQL query to analyze book statistics](images/GoodreadsSQLquerytoanalyzebookstatistics.png)
SQL query to analyze book statistics

### ISBN Group Analysis
![SQL query to analyze geographic and agency data](images/SQLquerytoanalyzegeographicandagencydata.png)
SQL query to analyze geographic and agency data

### Kulturpass Analysis
![Book and Price Analysis](images/BookandPriceAnalysisKulturpass.png)
Book and Price Analysis

### Libby Records Analysis
![Content and Publisher Analysis](images/LibbyBookandPriceAnalysis.png)
Content and Publisher Analysis

### Magazine Database Analysis
![Upload and File Analysis](images/MaganizeUploadandFileAnalysis.png)
Upload and File Analysis


## 📊 MariaDB Analysis Examples

### Analysis of record distribution across different sources
![Analysis of record distribution across different sources](images/Analysisofrecorddistributionacrossdifferentsources.png)
Combines data from multiple archive sources to compare coverage

### Analysis of record ID prefixes distribution
![Analysis of record ID prefixes distribution](images/AnalysisofrecordIDprefixesdistribution.png)
Examines the distribution of different types of identifiers

### Series analysis from LibGen
![Series analysis from LibGen](images/SeriesanalysisfromLibGen.png)
Analyzes the distribution and characteristics of book series

### Code prefix analysis
![Code prefix analysis](images/Codeprefixanalysis.png)
Examines the distribution of code prefixes across different lookup tables

### ZLib book analysis
![ZLib book analysis](images/ZLibbookanalysis.png)
Analyzes the distribution of books in ZLib by language and file type

### LibGen editions analysis
![LibGen editions analysis](images/LibGeneditionsanalysis.png)
Examines the temporal distribution of publications

### Archive metadata analysis
![Archive metadata analysis](images/ArchivemetadataanalysisSources.png)
Analyzes the distribution of records across different archive sources

### Series temporal analysis
![Series temporal analysis](images/SeriestemporalanalysisPublications.png)
Analyzes the temporal distribution of series publications

### File characteristics analysis
![File characteristics analysis](images/Filecharacteristicsanalysis.png)
Examines the distribution of file properties across different sources

### ISBN analysis
![ISBN analysis](images/ISBNanalysisCharacteristics.png)
Analyzes the distribution and characteristics of ISBN records

## 🚀 Quick Start

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd <project-directory>
   ```

2. **Build the Docker Image**
   ```bash
   docker-compose build
   ```

3. **Start the Environment**
   ```bash
   docker-compose up
   ```

4. **Access Jupyter**
   - Open the URL shown in the console
   - Default: http://localhost:8888

## 📊 Available Notebooks

### DATA Analysis
- `Elasticsearch_Queries.ipynb`: Elasticsearch Queries
- `ElasticsearchAUX_Queries.ipynb`: ElasticsearchAUX Queries
- `AAC_Queries.ipynb`: AAC Queries
- `Mariadb_Queries.ipynb`: MariaDB Queries

## 🔧 Configuration

### Memory Settings
- Default RAM requirement: 30GB
- DuckDB memory limit: 28GB (adjustable)
- Chunk size: 10MB (adjustable)
- Python environment: 3.12

### Performance Tuning
For systems with more resources:
- Increase chunk size for faster processing
- Adjust DuckDB memory limit
- Modify parallel processing parameters

### Docker Settings
- Port mapping: 8888:8888
- Volume mounts for data persistence
- Jupyter notebook directory mapping
