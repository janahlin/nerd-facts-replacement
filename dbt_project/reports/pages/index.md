---
title: Welcome to Star Wars facts
---

## Films
```sql all_films
  select
      film_id,
      film_title,
      director,
      release_date
  from nerd_facts.films
  order by release_date
```
<DataTable data={all_films} showLinkCol />

## Select a film
```sql films
  select
      film_id,
      film_title
  from nerd_facts.films
  order by release_date
```

<Dropdown data={films} name=selected_film label=film_title value=film_id>
  <DropdownOption value=0 valueLabel="Alla filmer"/>    
</Dropdown>

```sql characters_in_film
SELECT 
    p.person_name, 
    p.birth_year, 
    p.eye_color
FROM nerd_facts.people p
JOIN nerd_facts.film_characters fc ON p.person_id = fc.person_id
WHERE CASE WHEN '${inputs.selected_film.value}' = 0 THEN
        fc.film_id > '${inputs.selected_film.value}'
      ELSE
        fc.film_id = '${inputs.selected_film.value}'
      END
```

<DataTable data={characters_in_film} />

```sql planets_in_film
SELECT 
    pl.name, 
    pl.climate, 
    pl.population
FROM nerd_facts.planets pl
JOIN nerd_facts.film_planets fp ON pl.planet_id = fp.planet_id
WHERE CASE WHEN '${inputs.selected_film.value}' = 0 THEN
        fp.film_id > '${inputs.selected_film.value}'
      ELSE
        fp.film_id = '${inputs.selected_film.value}'
      END
```

<DataTable data={planets_in_film} />

```sql starships_in_film
SELECT 
    s.starship_name, 
    s.model, 
    s.manufacturer
FROM nerd_facts.starships s
JOIN nerd_facts.film_starships fs ON s.starship_id = fs.starship_id
WHERE CASE WHEN '${inputs.selected_film.value}' = 0 THEN
        fs.film_id > '${inputs.selected_film.value}'
      ELSE
        fs.film_id = '${inputs.selected_film.value}'
      END
```

<DataTable data={starships_in_film} />

```sql vehicles_in_film
SELECT 
    v.vehicle_name, 
    v.model, 
    v.manufacturer
FROM nerd_facts.vehicles v
JOIN nerd_facts.film_vehicles fv ON v.vehicle_id = fv.vehicle_id
WHERE CASE WHEN '${inputs.selected_film.value}' = 0 THEN
        fv.film_id > '${inputs.selected_film.value}'
      ELSE
        fv.film_id = '${inputs.selected_film.value}'
      END
```

<DataTable data={vehicles_in_film} />

```sql characters_by_film
 SELECT f.film_title,
         COUNT(fc.person_id) AS num_characters
  FROM nerd_facts.film_characters fc
  JOIN nerd_facts.films f ON fc.film_id = f.film_id
  GROUP BY f.film_title
  ORDER BY num_characters DESC
```

<DataTable data={characters_by_film} />

```sql top_species
  SELECT s.species_name,
         COUNT(*) AS appearances
  FROM nerd_facts.film_species fs
  JOIN nerd_facts.species s ON fs.species_id = s.species_id
  GROUP BY s.species_name
  ORDER BY appearances DESC
  LIMIT 5
```

<DataTable data={top_species} />