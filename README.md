# **Landscape Analysis with Twitter Data**

This repository contains all the code used for my research on how landscapes are described and perceived in Spanish-speaking Central and South America using Twitter data from 2012–2019. 
The study applies Geographic Information Retrieval (GIR), Natural Language Processing (NLP), and machine learning techniques to extract spatial, temporal, and thematic patterns.
The data is not available on GitHub since it was provided to me by the University of Zurich Language Center.

---

## **Table of Contents**
1. [Introduction](#introduction)
2. [Data and Methods](#data-and-methods)
3. [Repository Structure](#repository-structure)
4. [Results](#results)
---

## **Introduction**
This project aims to analyze how landscapes are discussed and perceived in Spanish-speaking regions of Central and South America. By leveraging a large dataset of tweets, this work explores:
- **Spatial Patterns**: Identifying hotspots where specific landscape terms are mentioned.
- **Temporal Patterns**: Understanding seasonal trends and event-based occurrences.
- **Thematic Patterns**: Unveiling co-occurring terms and emotional associations with landscapes.

---

## **Data and Methods**
- **Dataset**: 995 million tweets in Spanish (2012–2019).
- **Processing Steps**:
  1. Preprocessing and filtering tweets by landscape-related terms.
  2. Classification of tweets using a Random Forest machine learning model.
  3. Spatial, temporal, and thematic analyses to extract meaningful insights.
- **Tools Used**:
  - Python libraries: `pandas`, `geopandas`, `sklearn`, `spacy`, `matplotlib`, and more.
  - PostgreSQL for database management.

---

## **Repository Structure**
```plaintext
├── Analysis/                  # Scripts for data analysis
│   ├── CoOccurence/           # Co-occurrence analysis
│   │   ├── Co_Occurence.py    # Main script for co-occurrence analysis
│   │   └── visualize.py       # Visualization script for co-occurrence results
│   ├── Spatial/               # Spatial analysis
│   │   └── chi2_spatial.py    # Script for spatial analysis
│   └── Temporal/              # Temporal analysis
│       ├── Chi_2_Days.py      # Main temporal analysis script
│       ├── Chi_2_Days_Term.py # Analyze a specific term over time
│       └── Hotspot.py         # Identify temporal hotspots
├── Data/                      # Data
│   └── Spatial/               # Spatial data files 
├── Dataprocessing/            # Scripts for data preprocessing and ML
│   ├── 1_filtering.py         # Filter tweets by landscape terms
│   ├── 2_training.py          # Script for manual labeling and model training
│   ├── 3_machine_learning.py  # Classify tweets using Random Forest
│   ├── 4_sample.py            # Script to sample data for evaluation
│   └── 5_precision.py         # Evaluate precision of classification
├── README.md                  # Documentation for the repository
└── requirements.txt           # Python dependencies
```

---

## **Results**
Key results of this project include:
- Spatial maps highlighting hotspots of landscape-related tweets.
- Heatmaps showing seasonal and event-driven trends in landscape discussions.
- Word clouds and co-occurrence statistics visualizing thematic patterns.

---
