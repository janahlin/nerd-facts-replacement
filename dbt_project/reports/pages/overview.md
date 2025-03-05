---
title: Star Wars Data Overview  
description: A high-level overview of key Star Wars data insights.  
---

# 🌌 Star Wars Data Overview

Welcome to the **Star Wars Data Dashboard**, powered by **dbt + Evidence + DuckDB**. This page provides a quick overview of the most important insights from our dataset.

## 🎬 Films Overview  
```sql basic_film_info
SELECT COUNT(*) AS total_films, MIN(release_date) AS first_film, MAX(release_date) AS latest_film 
FROM films;
```

Totalt antal filmer gjorda är <Value data={basic_film_info} /> där den första kom ut <Value data={basic_film_info} column=first_film/> och den senaste <Value data={basic_film_info} column=latest_film/>


## 👤 Characters Overview
```sql total_characters
SELECT COUNT(*) AS total_characters FROM people;
```


## 🪐 Planets Overview
``` sql total_planets
SELECT COUNT(*) AS total_planets FROM planets;
```


## 🚀 Starships Overview

```sql total_starships
SELECT COUNT(*) AS total_starships FROM starships;
```


## 🏎️ Vehicles Overview

```sql total_vehicles
SELECT COUNT(*) AS total_vehicles FROM vehicles;
```
