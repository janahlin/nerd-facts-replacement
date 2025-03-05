---
title: Star Wars Data Overview  
description: A high-level overview of key Star Wars data insights.  
---

Welcome to the **Star Wars Data Dashboard**, powered by **dbt + Evidence + DuckDB**. This page provides a quick overview of the most important insights from our dataset.

# 📊 **Key Metrics**
```sql kpi_summary
SELECT *
from mart_film_summary
order by film_id;
```

<DataTable data={kpi_summary} >
    <Column id=film_id />
    <Column id=title />
    <Column id=director />
    <Column id=producer />
    <Column id=num_characters title="Characters"/>
    <Column id=num_planets title="Planets"/>
    <Column id=num_species title="Species"/>
    <Column id=num_starships title="Starships"/>
    <Column id=num_vehicles title="Vehicles"/>
</DataTable>

## 🎬 Films Overview  
```sql basic_film_info
SELECT COUNT(*) AS total_films, MIN(release_date) AS first_film, MAX(release_date) AS latest_film 
FROM films;
```

Totalt antal filmer gjorda är <Value data={basic_film_info} /> där den första kom ut <Value data={basic_film_info} column=first_film/> och den senaste <Value data={basic_film_info} column=latest_film/>

# 🌌 Star Wars Data Overview

```sql characters_by_gender
select 
    gender,
    count(*) as count
from people
group by gender
order by count desc
```
<BarChart 
    data={characters_by_gender} 
    x="gender" 
    y="count"
    title="Character Distribution by Gender" 
/>

```sql most_populated_planets
select 
    name,
    population
from planets
where population is not null
order by population desc
limit 10
```
<DataTable 
    data={most_populated_planets}
    title="Top 10 Most Populated Planets"
/>

```sql height_distribution
select 
    height_cm,
    count(*) as character_count
from people
where height_cm is not null
group by height_cm
order by height_cm
```
<LineChart 
    data={height_distribution}
    x="height_cm"
    y="character_count"
    title="Distribution of Character Heights"
/>

## Planet Analysis

```sql planet_climates
select 
    climate,
    count(*) as planet_count    
from planets
where climate != 'unknown'
group by climate
order by planet_count desc
```

<BarChart 
    data={planet_climates}
    x="climate"
    y="planet_count"
    title="Planets by Climate Type"
/>
