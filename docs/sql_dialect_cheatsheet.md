# SQL Dialect Cheatsheet: Snowflake → Trino Migration

This document captures every SQL syntax difference I encountered while migrating dbt models from Snowflake to Trino during this project.

---

## Date Functions

The most common source of errors during migration.

| Function | Snowflake | Trino |
|---|---|---|
| Date difference | `DATEDIFF('day', a, b)` | `date_diff('day', a, b)` |
| Add to date | `DATEADD('day', 7, d)` | `date_add('day', 7, d)` |
| Current date | `CURRENT_DATE()` | `current_date` |
| Current timestamp | `CURRENT_TIMESTAMP()` | `current_timestamp` |
| Truncate date | `DATE_TRUNC('month', d)` | `date_trunc('month', d)` |
| Extract part | `EXTRACT(month FROM d)` | `extract(month FROM d)` |

Key point: Trino drops the parentheses on `current_date` and 
`current_timestamp`. This caused silent failures in our mart models 
until we caught it during testing.

---

## Null Handling

| Function | Snowflake | Trino |
|---|---|---|
| Replace null with zero | `ZEROIFNULL(x)` | `coalesce(x, 0)` |
| Replace null with value | `NVL(x, y)` | `coalesce(x, y)` |
| Null if equal | `NULLIF(x, y)` | `nullif(x, y)` |

Snowflake has convenience functions like `ZEROIFNULL` that do not exist 
in Trino. Always replace them with the standard `coalesce` equivalent.

---

## String Functions

| Function | Snowflake | Trino |
|---|---|---|
| Concatenate | `x || y` or `CONCAT(x, y)` | `concat(x, y)` |
| String position | `CHARINDEX(sub, str)` | `strpos(str, sub)` |
| Aggregate to string | `LISTAGG(x, ',')` | `array_join(array_agg(x), ',')` |
| Left pad | `LPAD(x, 10, '0')` | `lpad(x, 10, '0')` |

Note: `LISTAGG` requires a full rewrite in Trino. It becomes a 
two-function pattern using `array_agg` wrapped in `array_join`.

---

## Conditional Functions

| Function | Snowflake | Trino |
|---|---|---|
| If condition | `IFF(cond, a, b)` | `if(cond, a, b)` |
| Case statement | Standard CASE | Standard CASE |
| Decode | `DECODE(x, v1, r1, default)` | Use CASE WHEN instead |

---

## JOIN Syntax

This was the most impactful finding in this migration.

Snowflake supports aliased columns in `USING` clauses:
```sql
-- Snowflake (works)
FROM invoices i
LEFT JOIN payments p USING (invoice_id)
-- Then reference as i.invoice_id or p.invoice_id
```

Trino does not resolve table aliases when using `USING`:
```sql
-- Trino (USING causes COLUMN_NOT_FOUND errors with aliases)
-- Must rewrite as explicit ON clause
FROM invoices i
LEFT JOIN payments p ON i.invoice_id = p.invoice_id
```

This affected all three intermediate models and was fixed by replacing 
every `USING` clause with an explicit `ON` condition.

---

## Semi-Structured Data

| Operation | Snowflake | Trino |
|---|---|---|
| JSON field access | `col:field` or `col['field']` | `json_extract_scalar(col, '$.field')` |
| Parse JSON | `PARSE_JSON(str)` | `json_parse(str)` |
| Array flatten | `FLATTEN(SPLIT(x, ','))` | `unnest(split(x, ','))` |

---

## Window Functions

Window functions are largely compatible between Snowflake and Trino. 
No changes were needed in our mart models.

```sql
-- Works in both Snowflake and Trino
rank() OVER (PARTITION BY vendor_category ORDER BY total_spend DESC)
```

---

## Key Lessons Learned

1. Always run `dbt compile` before `dbt run` when migrating — it catches syntax errors without executing against the database.

2. Function names in Trino are case-sensitive in some contexts. Use lowercase for all built-in functions to be safe.

3. The `USING` clause behavior difference between Snowflake and Trino is not documented clearly. Test every JOIN when migrating.

4. Date functions look similar across dialects but have subtle differences in argument order and naming. Always verify with a simple SELECT first.

5. Snowflake convenience functions like `ZEROIFNULL`, `IFF`, and `LISTAGG` have no direct Trino equivalents. Build a find-and-replace checklist before starting any migration.