# app.py
import streamlit as st
import pandas as pd
import altair as alt
from db_connect import get_connection
import queries
from datetime import datetime


# ----------------------------
# Page config
# ----------------------------
st.set_page_config(
    page_title="Local Food Wastage Management",
    page_icon="🍽",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ----------------------------
# Helpers
# ----------------------------


@st.cache_data(show_spinner=False)
def load_table(sql: str, params=None):
    conn = get_connection()
    try:
        df = pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()
    return df


def paginate_df(df: pd.DataFrame, key: str, rows_per_page: int = 10):
    if df.empty:
        st.info("No rows to show.")
        return
    total = len(df)
    pages = (total - 1) // rows_per_page + 1
    col_a, col_b, col_c = st.columns([1, 2, 1])
    with col_b:
        page = st.number_input(
            f"Page ({total} rows)", min_value=1, max_value=max(pages, 1),
            value=1, step=1, key=f"{key}_page"
        )
    start = (page - 1) * rows_per_page
    end = start + rows_per_page
    st.dataframe(df.iloc[start:end], use_container_width=True, height=360)


def kpi_metric(label, value):
    st.metric(label, value if value is not None else 0)


def to_csv_bytes(df: pd.DataFrame):
    return df.to_csv(index=False).encode("utf-8")


# ----------------------------
# Title
# ----------------------------
st.title("🍽 Local Food Wastage Management System")
st.caption(
    "Connect surplus food providers with receivers, reduce waste, and drive social good.")


# =========================================================
# LOAD CORE DATA (for filters & tables)
# =========================================================
providers_df = load_table("""
    SELECT Provider_ID, Name, Type, City, Contact, Address
    FROM providers
""")


receivers_df = load_table("""
    SELECT Receiver_ID, Name, Type, City, Contact
    FROM receivers
""")


food_df = load_table("""
    SELECT Food_ID, Food_Name, Quantity, Expiry_Date, Provider_ID, Provider_Type,
           Location, Food_Type, Meal_Type
    FROM food_listings
""")


claims_df = load_table("""
    SELECT Claim_ID, Food_ID, Receiver_ID, Status, Timestamp
    FROM claims
""")


# =========================================================
# SIDEBAR FILTERS
# =========================================================
st.sidebar.header("🔍 Filters")


all_cities = sorted(set(
    providers_df["City"].dropna().unique().tolist()
    + food_df["Location"].dropna().unique().tolist()
))
city_choice = st.sidebar.multiselect("City", options=all_cities, default=[])


provider_types = sorted(providers_df["Type"].dropna().unique().tolist())
provider_type_choice = st.sidebar.multiselect(
    "Provider Type", provider_types, default=[])


food_types = sorted(food_df["Food_Type"].dropna().unique().tolist())
food_type_choice = st.sidebar.multiselect("Food Type", food_types, default=[])


meal_types = sorted(food_df["Meal_Type"].dropna().unique().tolist())
meal_type_choice = st.sidebar.multiselect("Meal Type", meal_types, default=[])


claim_statuses = sorted(claims_df["Status"].dropna().unique().tolist())
claim_status_choice = st.sidebar.multiselect(
    "Claim Status", claim_statuses, default=[])


st.sidebar.markdown("---")
apply_filters_to_analytics = st.sidebar.checkbox(
    "Apply Filters to Analytics/KPIs", value=True)
st.sidebar.caption(
    "Use filters to refine data in Explore tab and optionally Analytics/KPIs.")


# Apply filters (non-destructive; copies) for Explore tab
f_providers = providers_df.copy()
f_receivers = receivers_df.copy()
f_food = food_df.copy()
f_claims = claims_df.copy()


if city_choice:
    f_providers = f_providers[f_providers["City"].isin(city_choice)]
    f_receivers = f_receivers[f_receivers["City"].isin(city_choice)]
    f_food = f_food[f_food["Location"].isin(city_choice)]


if provider_type_choice:
    f_providers = f_providers[f_providers["Type"].isin(provider_type_choice)]
    f_food = f_food[f_food["Provider_Type"].isin(provider_type_choice)]


if food_type_choice:
    f_food = f_food[f_food["Food_Type"].isin(food_type_choice)]


if meal_type_choice:
    f_food = f_food[f_food["Meal_Type"].isin(meal_type_choice)]


if claim_status_choice:
    f_claims = f_claims[f_claims["Status"].isin(claim_status_choice)]


# Optional: Display active filters summary
active_filters = []
if city_choice:
    active_filters.append(f"City={city_choice}")
if provider_type_choice:
    active_filters.append(f"Provider Type={provider_type_choice}")
if food_type_choice:
    active_filters.append(f"Food Type={food_type_choice}")
if meal_type_choice:
    active_filters.append(f"Meal Type={meal_type_choice}")
if claim_status_choice:
    active_filters.append(f"Claim Status={claim_status_choice}")
if active_filters:
    st.info("Filters applied: " + " | ".join(active_filters))


# =========================================================
# KPIs (now computed via queries with optional filters)
# =========================================================
# Decide which filter sets to pass to queries for KPIs/Analytics
filters_for_analytics = dict(
    cities=city_choice if apply_filters_to_analytics else [],
    provider_types=provider_type_choice if apply_filters_to_analytics else [],
    food_types=food_type_choice if apply_filters_to_analytics else [],
    meal_types=meal_type_choice if apply_filters_to_analytics else [],
    claim_statuses=claim_status_choice if apply_filters_to_analytics else [],
)


# KPI: total providers
kpi_providers_df = queries.count_providers(
    cities=filters_for_analytics["cities"],
    provider_types=filters_for_analytics["provider_types"]
)
total_providers = int(
    kpi_providers_df.iloc[0, 0]) if not kpi_providers_df.empty else 0


# KPI: total receivers
kpi_receivers_df = queries.count_receivers(
    cities=filters_for_analytics["cities"]
)
total_receivers = int(
    kpi_receivers_df.iloc[0, 0]) if not kpi_receivers_df.empty else 0


# KPI: total claims
kpi_claims_df = queries.count_claims(
    cities=filters_for_analytics["cities"],
    claim_statuses=filters_for_analytics["claim_statuses"]
)
total_claims = int(kpi_claims_df.iloc[0, 0]) if not kpi_claims_df.empty else 0


# KPI: total food quantity
kpi_food_qty_df = queries.total_food_quantity_filtered(
    cities=filters_for_analytics["cities"],
    provider_types=filters_for_analytics["provider_types"],
    food_types=filters_for_analytics["food_types"],
    meal_types=filters_for_analytics["meal_types"]
)
total_food_qty = int(
    kpi_food_qty_df.iloc[0, 0]) if not kpi_food_qty_df.empty else 0


col1, col2, col3, col4 = st.columns(4)
with col1:
    kpi_metric("🏢 Providers", total_providers)
with col2:
    kpi_metric("🙋 Receivers", total_receivers)
with col3:
    kpi_metric("🥘 Total Food Qty", total_food_qty)
with col4:
    kpi_metric("📦 Claims", total_claims)


st.markdown("---")


# =========================================================
# TABS
# =========================================================
tab1, tab2, tab3, tab4 = st.tabs(
    ["📈 Analytics", "🔎 Explore", "🛠 CRUD", "ℹ About"])


# =========================================================
# TAB 1: ANALYTICS (queries now accept filters)
# =========================================================
with tab1:
    st.subheader("Key Insights")

    # All queries accept optional filters (empty list means no filter)
    q_prov_by_city = queries.providers_per_city(
        cities=filters_for_analytics["cities"],
        provider_types=filters_for_analytics["provider_types"]
    )
    q_recv_by_city = queries.receivers_per_city(
        cities=filters_for_analytics["cities"]
    )
    q_top_types = queries.top_provider_types(
        cities=filters_for_analytics["cities"]
    )
    q_top_receivers = queries.top_receivers(
        cities=filters_for_analytics["cities"]
    )
    q_total_qty = kpi_food_qty_df  # already computed above
    q_city_most_list = queries.city_highest_listings(
        cities=filters_for_analytics["cities"],
        provider_types=filters_for_analytics["provider_types"],
        food_types=filters_for_analytics["food_types"],
        meal_types=filters_for_analytics["meal_types"]
    )
    q_common_types = queries.common_food_types(
        cities=filters_for_analytics["cities"]
    )
    q_claims_per_food = queries.claims_per_food(
        cities=filters_for_analytics["cities"],
        claim_statuses=filters_for_analytics["claim_statuses"]
    )
    q_top_prov_success = queries.top_providers_successful_claims(
        cities=filters_for_analytics["cities"]
    )
    q_status_dist = queries.claim_status_distribution(
        cities=filters_for_analytics["cities"]
    )
    q_avg_qty_recv = queries.avg_quantity_per_receiver(
        cities=filters_for_analytics["cities"]
    )
    q_most_meal = queries.most_claimed_meal_type(
        cities=filters_for_analytics["cities"]
    )
    q_total_per_provider = queries.total_quantity_per_provider(
        cities=filters_for_analytics["cities"]
    )
    q_cities_most_claims = queries.cities_with_most_claims(
        cities=filters_for_analytics["cities"]
    )

    # Row 1
    c1, c2, c3 = st.columns(3, gap="large")

    # 1) Providers per City (Top 10)
    with c1:
        st.caption("Top 10 Cities by Providers")
        if not q_prov_by_city.empty:
            top10 = q_prov_by_city.sort_values(
                "Provider_Count", ascending=False).head(10)
            chart = alt.Chart(top10).mark_bar().encode(
                x=alt.X('Provider_Count:Q', title='Providers'),
                y=alt.Y('City:N', sort='-x', title='City'),
                tooltip=[
                    alt.Tooltip('City:N', title='City'),
                    alt.Tooltip('Provider_Count:Q',
                                title='Providers', format=',d')
                ]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No provider data.")

    # 2) Claims Status Distribution — pie
    with c2:
        st.caption("Claims Status Distribution")
        if not q_status_dist.empty:
            pie = alt.Chart(q_status_dist).mark_arc().encode(
                theta=alt.Theta('Count:Q'),
                color=alt.Color('Status:N', legend=alt.Legend(title='Status')),
                tooltip=[
                    alt.Tooltip('Status:N', title='Status'),
                    alt.Tooltip('Count:Q', title='Count', format=',d')
                ]
            ).properties(height=320)
            st.altair_chart(pie, use_container_width=True)
        else:
            st.info("No claims data.")

    # 3) Most Claimed Meal Types
    with c3:
        st.caption("Most Claimed Meal Types")
        if not q_most_meal.empty:
            chart = alt.Chart(q_most_meal).mark_bar().encode(
                x=alt.X('Claim_Count:Q', title='Claims'),
                y=alt.Y('Meal_Type:N', sort='-x', title='Meal Type'),
                tooltip=[
                    alt.Tooltip('Meal_Type:N', title='Meal Type'),
                    alt.Tooltip('Claim_Count:Q', title='Claims', format=',d')
                ]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No meal type data.")

    st.markdown("---")

    # Row 2
    c4, c5, c6 = st.columns(3, gap="large")

    # 4) Common Food Types (Listings)
    with c4:
        st.caption("Common Food Types (Listings)")
        if not q_common_types.empty:
            ft = q_common_types.rename(columns={"Count": "Count_Type"})
            chart = alt.Chart(ft).mark_bar().encode(
                x=alt.X('Count_Type:Q', title='Listings'),
                y=alt.Y('Food_Type:N', sort='-x', title='Food Type'),
                tooltip=[
                    alt.Tooltip('Food_Type:N', title='Food Type'),
                    alt.Tooltip('Count_Type:Q', title='Listings', format=',d')
                ]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No food types found.")

    # 5) Top 10 Cities with Most Claims
    with c5:
        st.caption("Top 10 Cities with Most Claims")
        if not q_cities_most_claims.empty:
            cities10 = q_cities_most_claims.rename(
                columns={"Claim_Count": "Claims"}).head(10)
            chart = alt.Chart(cities10).mark_bar().encode(
                x=alt.X('Claims:Q', title='Claims'),
                y=alt.Y('City:N', sort='-x', title='City'),
                tooltip=[
                    alt.Tooltip('City:N', title='City'),
                    alt.Tooltip('Claims:Q', title='Claims', format=',d')
                ]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No city claim data.")

    # 6) Top Providers by Successful Claims
    with c6:
        st.caption("Top Providers by Successful Claims (Top 10)")
        if not q_top_prov_success.empty:
            top10succ = q_top_prov_success.head(10)
            chart = alt.Chart(top10succ).mark_bar().encode(
                x=alt.X('Successful_Claims:Q', title='Successful Claims'),
                y=alt.Y('Name:N', sort='-x', title='Provider'),
                tooltip=[
                    alt.Tooltip('Name:N', title='Provider'),
                    alt.Tooltip('Successful_Claims:Q',
                                title='Successful Claims', format=',d')
                ]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No successful claims.")

    st.markdown("---")
    st.subheader("Detailed Outputs (Compact Tables)")

    exp1 = st.expander("Providers per City")
    with exp1:
        paginate_df(q_prov_by_city, key="prov_city")
        if not q_prov_by_city.empty:
            st.download_button("⬇ Download CSV", data=to_csv_bytes(q_prov_by_city),
                               file_name="providers_per_city.csv")

    exp2 = st.expander("Receivers per City")
    with exp2:
        paginate_df(q_recv_by_city, key="recv_city")
        if not q_recv_by_city.empty:
            st.download_button("⬇ Download CSV", data=to_csv_bytes(q_recv_by_city),
                               file_name="receivers_per_city.csv")

    exp3 = st.expander("Provider Types")
    with exp3:
        paginate_df(q_top_types, key="prov_types")
        if not q_top_types.empty:
            st.download_button("⬇ Download CSV", data=to_csv_bytes(q_top_types),
                               file_name="provider_types.csv")

    exp4 = st.expander("Top Receivers by Claims")
    with exp4:
        paginate_df(q_top_receivers, key="top_receivers")
        if not q_top_receivers.empty:
            st.download_button("⬇ Download CSV", data=to_csv_bytes(q_top_receivers),
                               file_name="top_receivers.csv")

    exp5 = st.expander("City with Highest Listings")
    with exp5:
        paginate_df(q_city_most_list, key="city_list")
        if not q_city_most_list.empty:
            st.download_button("⬇ Download CSV", data=to_csv_bytes(q_city_most_list),
                               file_name="city_highest_listings.csv")

    exp6 = st.expander("Claims per Food")
    with exp6:
        paginate_df(q_claims_per_food, key="claims_food")
        if not q_claims_per_food.empty:
            st.download_button("⬇ Download CSV", data=to_csv_bytes(q_claims_per_food),
                               file_name="claims_per_food.csv")

    exp7 = st.expander("Claim Status Distribution")
    with exp7:
        paginate_df(q_status_dist, key="status_dist")
        if not q_status_dist.empty:
            st.download_button("⬇ Download CSV", data=to_csv_bytes(q_status_dist),
                               file_name="claim_status_distribution.csv")

    exp8 = st.expander("Avg Quantity per Receiver")
    with exp8:
        paginate_df(q_avg_qty_recv, key="avg_qty_recv")
        if not q_avg_qty_recv.empty:
            st.download_button("⬇ Download CSV", data=to_csv_bytes(q_avg_qty_recv),
                               file_name="avg_quantity_per_receiver.csv")

    exp9 = st.expander("Total Quantity per Provider")
    with exp9:
        paginate_df(q_total_per_provider, key="qty_per_provider")
        if not q_total_per_provider.empty:
            st.download_button("⬇ Download CSV", data=to_csv_bytes(q_total_per_provider),
                               file_name="total_quantity_per_provider.csv")


# The rest of the code remains unchanged...
# (Tabs 2-4 and CRUD section unchanged here for brevity)


# =========================================================
# TAB 2: EXPLORE (filtered lists + contact details)
# =========================================================
with tab2:
    st.subheader("Explore Data (Filtered)")

    c1, c2 = st.columns(2)
    with c1:
        st.caption("Providers (Contact Ready)")
        prov_view = f_providers[["Provider_ID", "Name",
                                 "Type", "City", "Contact"]].sort_values("City")
        paginate_df(prov_view, key="explore_providers")
        if not prov_view.empty:
            st.download_button("⬇ Download Providers CSV", data=to_csv_bytes(prov_view),
                               file_name="providers_filtered.csv")

    with c2:
        st.caption("Receivers")
        recv_view = f_receivers[["Receiver_ID", "Name",
                                 "Type", "City", "Contact"]].sort_values("City")
        paginate_df(recv_view, key="explore_receivers")
        if not recv_view.empty:
            st.download_button("⬇ Download Receivers CSV", data=to_csv_bytes(recv_view),
                               file_name="receivers_filtered.csv")

    st.caption("Food Listings")
    food_view = f_food[[
        "Food_ID", "Food_Name", "Quantity", "Expiry_Date", "Provider_ID",
        "Provider_Type", "Location", "Food_Type", "Meal_Type"
    ]].sort_values(["Location", "Expiry_Date"])
    paginate_df(food_view, key="explore_food")
    if not food_view.empty:
        st.download_button("⬇ Download Food Listings CSV", data=to_csv_bytes(food_view),
                           file_name="food_listings_filtered.csv")

    st.caption("Claims")
    claims_view = f_claims[["Claim_ID", "Food_ID", "Receiver_ID", "Status", "Timestamp"]].sort_values(
        "Timestamp", ascending=False
    )
    paginate_df(claims_view, key="explore_claims")
    if not claims_view.empty:
        st.download_button("⬇ Download Claims CSV", data=to_csv_bytes(claims_view),
                           file_name="claims_filtered.csv")

# =========================================================
# TAB 3: CRUD (Providers, Receivers, Food Listings, Claims)
# =========================================================
with tab3:
    st.subheader("Manage Data (CRUD)")

    crud_entity = st.selectbox(
        "Choose Entity", ["Food Listings", "Providers", "Receivers", "Claims"])

    conn = get_connection()
    cur = conn.cursor()

    try:
        if crud_entity == "Food Listings":
            action = st.radio(
                "Action", ["Add", "Update", "Delete"], horizontal=True)

            if action == "Add":
                with st.form("add_food_form", clear_on_submit=True):
                    food_name = st.text_input("Food Name", max_chars=255)
                    qty = st.number_input("Quantity", min_value=1, step=1)
                    expiry = st.date_input("Expiry Date")
                    provider_id = st.number_input(
                        "Provider ID", min_value=1, step=1)
                    location = st.text_input("Location (City)", max_chars=100)
                    provider_type = st.selectbox("Provider Type", [
                        "Restaurant", "Grocery Store", "Supermarket", "Catering Service"
                    ])
                    food_type = st.selectbox(
                        "Food Type", ["Vegetarian", "Non-Vegetarian", "Vegan"])
                    meal_type = st.selectbox(
                        "Meal Type", ["Breakfast", "Lunch", "Dinner", "Snacks"])
                    submitted = st.form_submit_button("➕ Add Food")

                if submitted:
                    if int(provider_id) not in providers_df["Provider_ID"].values:
                        st.error(
                            "❌ Invalid Provider ID (must exist in providers).")
                    elif not food_name.strip() or not location.strip():
                        st.error("❌ Food Name and Location cannot be empty.")
                    else:
                        try:
                            cur.execute("""
                                INSERT INTO food_listings
                                (Food_Name, Quantity, Expiry_Date, Provider_ID, Provider_Type, Location, Food_Type, Meal_Type)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            """, (food_name.strip(), qty, expiry, int(provider_id), provider_type, location.strip(), food_type, meal_type))
                            conn.commit()
                            st.success("Food listing added ✅")
                        except Exception as e:
                            conn.rollback()
                            st.error(f"Error: {e}")

            elif action == "Update":
                food_id = st.number_input(
                    "Food_ID to Update", min_value=1, step=1)
                with st.form("upd_food_form"):
                    new_qty = st.number_input(
                        "New Quantity", min_value=0, step=1)
                    new_expiry = st.date_input("New Expiry Date")
                    new_location = st.text_input(
                        "New Location (City)", max_chars=100)
                    new_meal = st.selectbox(
                        "New Meal Type", ["Breakfast", "Lunch", "Dinner", "Snacks"])
                    submitted = st.form_submit_button("✏ Update")

                if submitted:
                    try:
                        cur.execute("""
                            UPDATE food_listings
                            SET Quantity=%s, Expiry_Date=%s, Location=%s, Meal_Type=%s
                            WHERE Food_ID=%s
                        """, (new_qty, new_expiry, new_location.strip(), new_meal, int(food_id)))
                        conn.commit()
                        st.success("Food listing updated ✅")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {e}")

            elif action == "Delete":
                with st.form("del_food_form"):
                    food_id = st.number_input(
                        "Food_ID to Delete", min_value=1, step=1)
                    submitted = st.form_submit_button("🗑 Delete")
                if submitted:
                    try:
                        cur.execute(
                            "DELETE FROM food_listings WHERE Food_ID=%s", (int(food_id),))
                        conn.commit()
                        st.warning("Food listing deleted ⚠")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {e}")

        elif crud_entity == "Providers":
            action = st.radio(
                "Action", ["Add", "Update", "Delete"], horizontal=True)

            if action == "Add":
                with st.form("add_prov_form", clear_on_submit=True):
                    name = st.text_input("Provider Name", max_chars=255)
                    ptype = st.selectbox(
                        "Type", ["Restaurant", "Grocery Store", "Supermarket", "Catering Service"])
                    address = st.text_area("Address")
                    city = st.text_input("City", max_chars=100)
                    contact = st.text_input("Contact", max_chars=100)
                    submitted = st.form_submit_button("➕ Add Provider")

                if submitted:
                    if not name.strip() or not city.strip():
                        st.error("❌ Name and City cannot be empty.")
                    else:
                        try:
                            next_id_df = load_table(
                                "SELECT COALESCE(MAX(Provider_ID),0)+1 AS next_id FROM providers")
                            next_id = int(
                                next_id_df.iloc[0, 0]) if not next_id_df.empty else 1
                            cur.execute("""
                                INSERT INTO providers (Provider_ID, Name, Type, Address, City, Contact)
                                VALUES (%s,%s,%s,%s,%s,%s)
                            """, (next_id, name.strip(), ptype, address.strip(), city.strip(), contact.strip()))
                            conn.commit()
                            st.success(f"Provider added with ID {next_id} ✅")
                        except Exception as e:
                            conn.rollback()
                            st.error(f"Error: {e}")

            elif action == "Update":
                pid = st.number_input(
                    "Provider_ID to Update", min_value=1, step=1)
                with st.form("upd_prov_form"):
                    name = st.text_input("New Name", max_chars=255)
                    ptype = st.selectbox(
                        "New Type", ["Restaurant", "Grocery Store", "Supermarket", "Catering Service"])
                    address = st.text_area("New Address")
                    city = st.text_input("New City", max_chars=100)
                    contact = st.text_input("New Contact", max_chars=100)
                    submitted = st.form_submit_button("✏ Update")

                if submitted:
                    try:
                        cur.execute("""
                            UPDATE providers
                            SET Name=%s, Type=%s, Address=%s, City=%s, Contact=%s
                            WHERE Provider_ID=%s
                        """, (name.strip(), ptype, address.strip(), city.strip(), contact.strip(), int(pid)))
                        conn.commit()
                        st.success("Provider updated ✅")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {e}")

            elif action == "Delete":
                with st.form("del_prov_form"):
                    pid = st.number_input(
                        "Provider_ID to Delete", min_value=1, step=1)
                    st.caption(
                        "⚠ Deleting a provider may cascade-delete related food_listings if FK is ON DELETE CASCADE.")
                    submitted = st.form_submit_button("🗑 Delete Provider")
                if submitted:
                    try:
                        cur.execute(
                            "DELETE FROM providers WHERE Provider_ID=%s", (int(pid),))
                        conn.commit()
                        st.warning("Provider deleted ⚠")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {e}")

        elif crud_entity == "Receivers":
            action = st.radio(
                "Action", ["Add", "Update", "Delete"], horizontal=True)

            if action == "Add":
                with st.form("add_recv_form", clear_on_submit=True):
                    name = st.text_input("Receiver Name", max_chars=255)
                    rtype = st.selectbox(
                        "Type", ["NGO", "Shelter", "Charity", "Individual"])
                    city = st.text_input("City", max_chars=100)
                    contact = st.text_input("Contact", max_chars=100)
                    submitted = st.form_submit_button("➕ Add Receiver")

                if submitted:
                    if not name.strip() or not city.strip():
                        st.error("❌ Name and City cannot be empty.")
                    else:
                        try:
                            next_id_df = load_table(
                                "SELECT COALESCE(MAX(Receiver_ID),0)+1 AS next_id FROM receivers")
                            next_id = int(
                                next_id_df.iloc[0, 0]) if not next_id_df.empty else 1
                            cur.execute("""
                                INSERT INTO receivers (Receiver_ID, Name, Type, City, Contact)
                                VALUES (%s,%s,%s,%s,%s)
                            """, (next_id, name.strip(), rtype, city.strip(), contact.strip()))
                            conn.commit()
                            st.success(f"Receiver added with ID {next_id} ✅")
                        except Exception as e:
                            conn.rollback()
                            st.error(f"Error: {e}")

            elif action == "Update":
                rid = st.number_input(
                    "Receiver_ID to Update", min_value=1, step=1)
                with st.form("upd_recv_form"):
                    name = st.text_input("New Name", max_chars=255)
                    rtype = st.selectbox(
                        "New Type", ["NGO", "Shelter", "Charity", "Individual"])
                    city = st.text_input("New City", max_chars=100)
                    contact = st.text_input("New Contact", max_chars=100)
                    submitted = st.form_submit_button("✏ Update")

                if submitted:
                    try:
                        cur.execute("""
                            UPDATE receivers
                            SET Name=%s, Type=%s, City=%s, Contact=%s
                            WHERE Receiver_ID=%s
                        """, (name.strip(), rtype, city.strip(), contact.strip(), int(rid)))
                        conn.commit()
                        st.success("Receiver updated ✅")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {e}")

            elif action == "Delete":
                with st.form("del_recv_form"):
                    rid = st.number_input(
                        "Receiver_ID to Delete", min_value=1, step=1)
                    st.caption(
                        "⚠ Deleting a receiver may fail if claims reference it (unless FK ON DELETE CASCADE is set).")
                    submitted = st.form_submit_button("🗑 Delete Receiver")
                if submitted:
                    try:
                        cur.execute(
                            "DELETE FROM receivers WHERE Receiver_ID=%s", (int(rid),))
                        conn.commit()
                        st.warning("Receiver deleted ⚠")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {e}")

        elif crud_entity == "Claims":
            action = st.radio(
                "Action", ["Add", "Update", "Delete"], horizontal=True)

            if action == "Add":
                with st.form("add_claim_form", clear_on_submit=True):
                    food_id = st.number_input("Food_ID", min_value=1, step=1)
                    receiver_id = st.number_input(
                        "Receiver_ID", min_value=1, step=1)
                    status = st.selectbox(
                        "Status", ["Pending", "Completed", "Cancelled"])
                    col_dt1, col_dt2 = st.columns(2)
                    with col_dt1:
                        ts_date = st.date_input(
                            "Date", value=datetime.now().date())
                    with col_dt2:
                        ts_time = st.time_input(
                            "Time", value=datetime.now().time().replace(microsecond=0))
                    submitted = st.form_submit_button("➕ Add Claim")

                if submitted:
                    if int(food_id) not in food_df["Food_ID"].values:
                        st.error(
                            "❌ Invalid Food_ID (must exist in Food Listings).")
                    elif int(receiver_id) not in receivers_df["Receiver_ID"].values:
                        st.error(
                            "❌ Invalid Receiver_ID (must exist in Receivers).")
                    else:
                        try:
                            next_id_df = load_table(
                                "SELECT COALESCE(MAX(Claim_ID),0)+1 AS next_id FROM claims")
                            next_id = int(
                                next_id_df.iloc[0, 0]) if not next_id_df.empty else 1
                            timestamp = datetime.combine(
                                ts_date, ts_time).strftime("%Y-%m-%d %H:%M:%S")
                            cur.execute("""
                                INSERT INTO claims (Claim_ID, Food_ID, Receiver_ID, Status, Timestamp)
                                VALUES (%s,%s,%s,%s,%s)
                            """, (next_id, int(food_id), int(receiver_id), status, timestamp))
                            conn.commit()
                            st.success(f"Claim added with ID {next_id} ✅")
                        except Exception as e:
                            conn.rollback()
                            st.error(f"Error: {e}")

            elif action == "Update":
                with st.form("claim_status_form"):
                    cid = st.number_input(
                        "Claim_ID to Update", min_value=1, step=1)
                    new_status = st.selectbox(
                        "New Status", ["Pending", "Completed", "Cancelled"])
                    submitted = st.form_submit_button("✏ Update")
                if submitted:
                    try:
                        cur.execute(
                            "UPDATE claims SET Status=%s WHERE Claim_ID=%s", (new_status, int(cid)))
                        conn.commit()
                        st.success("Claim status updated ✅")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {e}")

            elif action == "Delete":
                with st.form("del_claim_form"):
                    cid = st.number_input(
                        "Claim_ID to Delete", min_value=1, step=1)
                    submitted = st.form_submit_button("🗑 Delete Claim")
                if submitted:
                    try:
                        cur.execute(
                            "DELETE FROM claims WHERE Claim_ID=%s", (int(cid),))
                        conn.commit()
                        st.warning("Claim deleted ⚠")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {e}")

    finally:
        try:
            cur.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass

# =========================================================
# TAB 4: ABOUT / PROJECT DOC
# =========================================================
with tab4:
    st.subheader("Project Overview")
    st.markdown("""
**Domain:** Food Management • Waste Reduction • Social Good  
**Technologies:** Python, SQL (MySQL), Streamlit, Data Analysis

**Problem Statement:**  
Large volumes of surplus food go to waste every day, while food insecurity affects many vulnerable populations.

**Solution Overview:**  
This platform bridges the gap by enabling **providers** (restaurants, stores) to list surplus food and **receivers** (NGOs, shelters, individuals) to claim it efficiently. The user-friendly Streamlit interface features powerful **filters**, interactive **visualizations**, and comprehensive **CRUD** operations for seamless data management.

**Key Features:**  
- Real-time KPIs and visually engaging charts for actionable insights  
- Dynamic filters by *city*, *provider type*, *food type*, *meal type*, and *claim status*  
- Contact-ready, easy-to-navigate tables for providers and receivers  
- Full CRUD functionality for Providers, Receivers, Food Listings, and Claims, with referential integrity  
- Concise query outputs with CSV download option for data export  

**How to Use:**  
1. Utilize the left sidebar filters to dynamically narrow down the data.  
2. Explore detailed records to connect with providers and receivers, and review food listings and claims.  
3. Analyze data via KPIs, charts, and summary tables based on SQL queries.  
4. Manage the database effortlessly using the CRUD interface—add, update, or delete records, while foreign key constraints ensure consistency.

*Developed by Chetan Upadhayay*
    """)
