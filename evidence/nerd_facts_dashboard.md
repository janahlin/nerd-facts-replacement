# Nerd Facts Dashboard
This dashboard presents fun facts from various APIs.

## Star Wars Characters
chart:
  type: bar
  data:
    sql: |
      SELECT name, height FROM swapi_people ORDER BY height DESC
  x: name
  y: height
