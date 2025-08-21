# queries.py
import pandas as pd
from db_connect import get_connection

# -------------
# Helper: build WHERE from filters with params
# -------------


def _in_clause(col, values, name, params):
    """
    Returns SQL snippet like 'col IN %(name)s' and updates params dict with tuple(values).
    If values is empty or None, returns '' and does not modify params.
    """
    if values:
        params[name] = tuple(values)
        return f"{col} IN %({name})s"
    return ""


def _and_join(conditions):
    conds = [c for c in conditions if c]
    if not conds:
        return ""
    return " WHERE " + " AND ".join(conds)

# -------------
# KPI helpers
# -------------


def count_providers(cities=None, provider_types=None):
    conn = get_connection()
    try:
        params = {}
        where = _and_join([
            _in_clause("City", cities, "cities", params),
            _in_clause("Type", provider_types, "ptypes", params),
        ])
        sql = f"""
            SELECT COUNT(*) AS total_providers
            FROM providers
            {where}
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


def count_receivers(cities=None):
    conn = get_connection()
    try:
        params = {}
        where = _and_join([
            _in_clause("City", cities, "cities", params),
        ])
        sql = f"""
            SELECT COUNT(*) AS total_receivers
            FROM receivers
            {where}
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


def count_claims(cities=None, claim_statuses=None):
    """
    If cities provided, filter by receivers.City (claims -> receivers).
    """
    conn = get_connection()
    try:
        params = {}
        joins = ""
        conditions = []
        if cities:
            joins += " JOIN receivers r ON c.Receiver_ID = r.Receiver_ID "
            conditions.append(_in_clause("r.City", cities, "cities", params))
        if claim_statuses:
            conditions.append(_in_clause(
                "c.Status", claim_statuses, "cstats", params))
        where = _and_join(conditions)

        sql = f"""
            SELECT COUNT(*) AS total_claims
            FROM claims c
            {joins}
            {where}
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


def total_food_quantity_filtered(cities=None, provider_types=None, food_types=None, meal_types=None):
    conn = get_connection()
    try:
        params = {}
        conditions = [
            _in_clause("Location", cities, "cities", params),
            _in_clause("Provider_Type", provider_types, "ptypes", params),
            _in_clause("Food_Type", food_types, "ftypes", params),
            _in_clause("Meal_Type", meal_types, "mtypes", params),
        ]
        where = _and_join(conditions)
        sql = f"""
            SELECT COALESCE(SUM(Quantity), 0) AS total_qty
            FROM food_listings
            {where}
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 1. Providers per City
# -------------


def providers_per_city(cities=None, provider_types=None):
    conn = get_connection()
    try:
        params = {}
        conditions = [
            _in_clause("City", cities, "cities", params),
            _in_clause("Type", provider_types, "ptypes", params),
        ]
        where = _and_join(conditions)
        sql = f"""
            SELECT City, COUNT(*) AS Provider_Count
            FROM providers
            {where}
            GROUP BY City
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 2. Receivers per City
# -------------


def receivers_per_city(cities=None):
    conn = get_connection()
    try:
        params = {}
        where = _and_join([
            _in_clause("City", cities, "cities", params),
        ])
        sql = f"""
            SELECT City, COUNT(*) AS Receiver_Count
            FROM receivers
            {where}
            GROUP BY City
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 3. Provider types contributing most
# -------------


def top_provider_types(cities=None):
    """
    If cities provided, filter providers by providers.City.
    """
    conn = get_connection()
    try:
        params = {}
        where = _and_join([
            _in_clause("City", cities, "cities", params),
        ])
        sql = f"""
            SELECT Type, COUNT(*) AS Contribution_Count
            FROM providers
            {where}
            GROUP BY Type
            ORDER BY Contribution_Count DESC
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 4. Provider contacts
# -------------


def provider_contacts(cities=None):
    conn = get_connection()
    try:
        params = {}
        where = _and_join([
            _in_clause("City", cities, "cities", params),
        ])
        sql = f"""
            SELECT Name, City, Contact
            FROM providers
            {where}
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 5. Top receivers by claims
# -------------


def top_receivers(cities=None, claim_statuses=None):
    """
    If cities provided, filter by receivers.City.
    claim_statuses filters claims.Status.
    """
    conn = get_connection()
    try:
        params = {}
        conditions = []
        if cities:
            conditions.append(_in_clause("r.City", cities, "cities", params))
        if claim_statuses:
            conditions.append(_in_clause(
                "c.Status", claim_statuses, "cstats", params))
        where = _and_join(conditions)

        sql = f"""
            SELECT r.Name, r.City, COUNT(c.Claim_ID) AS Total_Claims
            FROM receivers r
            JOIN claims c ON r.Receiver_ID = c.Receiver_ID
            {where}
            GROUP BY r.Name, r.City
            ORDER BY Total_Claims DESC
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 6. Total food quantity (unfiltered) - kept for compatibility
# -------------


def total_food_quantity():
    conn = get_connection()
    try:
        sql = "SELECT SUM(Quantity) AS Total_Quantity FROM food_listings"
        return pd.read_sql(sql, conn)
    finally:
        conn.close()

# -------------
# 7. City with highest listings
# -------------


def city_highest_listings(cities=None, provider_types=None, food_types=None, meal_types=None):
    conn = get_connection()
    try:
        params = {}
        conditions = [
            _in_clause("Location", cities, "cities", params),
            _in_clause("Provider_Type", provider_types, "ptypes", params),
            _in_clause("Food_Type", food_types, "ftypes", params),
            _in_clause("Meal_Type", meal_types, "mtypes", params),
        ]
        where = _and_join(conditions)
        sql = f"""
            SELECT Location AS City, COUNT(*) AS Listings
            FROM food_listings
            {where}
            GROUP BY Location
            ORDER BY Listings DESC
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 8. Common food types
# -------------


def common_food_types(cities=None):
    """
    If cities provided, filter food_listings by Location.
    """
    conn = get_connection()
    try:
        params = {}
        where = _and_join([
            _in_clause("Location", cities, "cities", params),
        ])
        sql = f"""
            SELECT Food_Type, COUNT(*) AS Count
            FROM food_listings
            {where}
            GROUP BY Food_Type
            ORDER BY Count DESC
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 9. Claims per food item
# -------------


def claims_per_food(cities=None, claim_statuses=None):
    """
    If cities provided, filter by receivers.City (path: claims -> receivers)
    and also allow claim status filter.
    """
    conn = get_connection()
    try:
        params = {}
        joins = "JOIN claims c ON f.Food_ID = c.Food_ID"
        joins += " JOIN receivers r ON c.Receiver_ID = r.Receiver_ID"
        conditions = []
        if cities:
            conditions.append(_in_clause("r.City", cities, "cities", params))
        if claim_statuses:
            conditions.append(_in_clause(
                "c.Status", claim_statuses, "cstats", params))
        where = _and_join(conditions)

        sql = f"""
            SELECT f.Food_Name, COUNT(c.Claim_ID) AS Claim_Count
            FROM food_listings f
            {joins}
            {where}
            GROUP BY f.Food_Name
            ORDER BY Claim_Count DESC
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 10. Top providers by successful claims
# -------------


def top_providers_successful_claims(cities=None):
    """
    If cities provided, filter by receivers.City (claims path) OR providers.City?
    The more consistent approach is to filter by receivers.City, since claims represent receivers.
    But often for provider success, city of provider is relevant.
    Here, we’ll filter by providers.City to reflect provider geography.
    """
    conn = get_connection()
    try:
        params = {}
        conditions = ["c.Status = 'Completed'"]
        if cities:
            conditions.append(_in_clause("p.City", cities, "cities", params))
        where = _and_join(conditions)

        sql = f"""
            SELECT p.Name, COUNT(c.Claim_ID) AS Successful_Claims
            FROM providers p
            JOIN food_listings f ON p.Provider_ID = f.Provider_ID
            JOIN claims c ON f.Food_ID = c.Food_ID
            {where}
            GROUP BY p.Name
            ORDER BY Successful_Claims DESC
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 11. Claim status distribution
# -------------


def claim_status_distribution(cities=None):
    """
    If cities provided, filter by receivers.City (claims -> receivers).
    """
    conn = get_connection()
    try:
        params = {}
        joins = ""
        conditions = []
        if cities:
            joins = " JOIN receivers r ON c.Receiver_ID = r.Receiver_ID "
            conditions.append(_in_clause("r.City", cities, "cities", params))
        where = _and_join(conditions)

        sql = f"""
            SELECT c.Status, COUNT(*) AS Count
            FROM claims c
            {joins}
            {where}
            GROUP BY c.Status
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 12. Average quantity claimed per receiver
# -------------


def avg_quantity_per_receiver(cities=None):
    """
    If cities provided, filter by receivers.City.
    """
    conn = get_connection()
    try:
        params = {}
        conditions = []
        if cities:
            conditions.append(_in_clause("r.City", cities, "cities", params))
        where = _and_join(conditions)

        sql = f"""
            SELECT r.Name, AVG(f.Quantity) AS Avg_Quantity
            FROM receivers r
            JOIN claims c ON r.Receiver_ID = c.Receiver_ID
            JOIN food_listings f ON c.Food_ID = f.Food_ID
            {where}
            GROUP BY r.Name
            ORDER BY Avg_Quantity DESC
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 13. Most claimed meal type
# -------------


def most_claimed_meal_type(cities=None):
    """
    If cities provided, filter by receivers.City through claims.
    """
    conn = get_connection()
    try:
        params = {}
        joins = "JOIN claims c ON f.Food_ID = c.Food_ID"
        joins += " JOIN receivers r ON c.Receiver_ID = r.Receiver_ID"
        conditions = []
        if cities:
            conditions.append(_in_clause("r.City", cities, "cities", params))
        where = _and_join(conditions)

        sql = f"""
            SELECT f.Meal_Type, COUNT(c.Claim_ID) AS Claim_Count
            FROM food_listings f
            {joins}
            {where}
            GROUP BY f.Meal_Type
            ORDER BY Claim_Count DESC
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 14. Total quantity donated by each provider
# -------------


def total_quantity_per_provider(cities=None):
    """
    If cities provided, filter by providers.City to reflect provider-based totals.
    """
    conn = get_connection()
    try:
        params = {}
        conditions = []
        if cities:
            conditions.append(_in_clause("p.City", cities, "cities", params))
        where = _and_join(conditions)

        sql = f"""
            SELECT p.Name, SUM(f.Quantity) AS Total_Quantity
            FROM providers p
            JOIN food_listings f ON p.Provider_ID = f.Provider_ID
            {where}
            GROUP BY p.Name
            ORDER BY Total_Quantity DESC
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

# -------------
# 15. Cities with most claims
# -------------


def cities_with_most_claims(cities=None):
    """
    If cities provided, this becomes a self-filter; typically not required,
    but we allow narrowing to a subset of cities to keep behavior consistent.
    """
    conn = get_connection()
    try:
        params = {}
        conditions = []
        if cities:
            conditions.append(_in_clause("r.City", cities, "cities", params))
        where = _and_join(conditions)

        sql = f"""
            SELECT r.City, COUNT(c.Claim_ID) AS Claim_Count
            FROM receivers r
            JOIN claims c ON r.Receiver_ID = c.Receiver_ID
            {where}
            GROUP BY r.City
            ORDER BY Claim_Count DESC
        """
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()
