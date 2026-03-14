from typing import Optional
from langchain_core.tools import tool
from app.data_loader import (
    get_dataframe,
    get_unique_cities,
    get_unique_property_types,
    get_districts_for_city,
    get_price_range,
)


@tool
def search_properties(
    city: Optional[str] = None,
    district: Optional[str] = None,
    property_type: Optional[str] = None,
    transaction_type: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_bedrooms: Optional[int] = None,
    min_bathrooms: Optional[int] = None,
    min_area: Optional[float] = None,
    max_area: Optional[float] = None,
    furnished: Optional[bool] = None,
) -> str:
    """Search for real estate properties based on filters.

    Use this tool when you have gathered enough preferences from the customer
    to perform a search. All parameters are optional — pass only the ones
    the customer specified.

    Args:
        city: City name (e.g. 'جدة', 'الرياض', 'الدمام').
        district: District/neighborhood name.
        property_type: Type of property (Villa, Apartment, Land, Building,
                       Store, Office, Room, Farm, Esterahah, Warehouse, House).
        transaction_type: 'Sell' for buying or 'Rental' for renting.
        min_price: Minimum price in SAR.
        max_price: Maximum price in SAR.
        min_bedrooms: Minimum number of bedrooms.
        min_bathrooms: Minimum number of bathrooms.
        min_area: Minimum area in square meters.
        max_area: Maximum area in square meters.
        furnished: True for furnished only, False for unfurnished only.

    Returns:
        A formatted string of up to 5 matching properties, or a
        message indicating no results were found.
    """
    df = get_dataframe()
    mask = df["price"].notna()  # baseline: valid price

    if city:
        mask &= df["city"].str.contains(city, case=False, na=False)
    if district:
        mask &= df["district"].str.contains(district, case=False, na=False)
    if property_type:
        mask &= df["property_type"].str.lower() == property_type.strip().lower()
    if transaction_type:
        mask &= df["transaction_type"].str.lower() == transaction_type.strip().lower()
    if min_price is not None:
        mask &= df["price"] >= min_price
    if max_price is not None:
        mask &= df["price"] <= max_price
    if min_bedrooms is not None:
        mask &= df["bedrooms"] >= min_bedrooms
    if min_bathrooms is not None:
        mask &= df["bathrooms"] >= min_bathrooms
    if min_area is not None:
        mask &= df["area"] >= min_area
    if max_area is not None:
        mask &= df["area"] <= max_area
    if furnished is not None:
        val = "Yes" if furnished else "No"
        mask &= df["has_furnished"] == val

    results = df.loc[mask]
    total = len(results)

    if total == 0:
        return "No properties found matching these criteria. Try relaxing some filters (e.g. increase budget, remove district, or change property type)."

    # Return top 5 sorted by price ascending
    top = results.sort_values("price").head(5)

    lines = [f"Found {total} properties. Here are the top 5:\n"]
    for _, row in top.iterrows():
        furnished_str = "Furnished" if row.get("has_furnished") == "Yes" else "Unfurnished"
        ac_str = "AC" if row.get("has_air_condition") == "Yes" else "No AC"
        area_str = f"{int(row['area'])} sqm" if pd.notna(row.get("area")) else "N/A"
        lines.append(
            f"• ID: {row['row_id']} | {row['property_type']} | {row['city']} - {row['district']}\n"
            f"  Price: {int(row['price']):,} SAR | {row['transaction_type']}\n"
            f"  Bedrooms: {int(row['bedrooms']) if pd.notna(row.get('bedrooms')) else 'N/A'} | "
            f"Bathrooms: {int(row['bathrooms']) if pd.notna(row.get('bathrooms')) else 'N/A'} | "
            f"Area: {area_str}\n"
            f"  {furnished_str} | {ac_str} | Age: {int(row['age']) if pd.notna(row.get('age')) else 'N/A'} yrs"
        )

    return "\n".join(lines)


import pandas as pd


@tool
def get_property_details(row_id: int) -> str:
    """Get full details for a specific property by its row_id.

    Use this when a customer wants more information about a specific
    property from the search results.

    Args:
        row_id: The unique property identifier (row_id from search results).

    Returns:
        Detailed property information or an error message.
    """
    df = get_dataframe()
    match = df[df["row_id"] == row_id]
    if match.empty:
        return f"No property found with ID {row_id}."

    row = match.iloc[0]
    details = {
        "Property ID": row["row_id"],
        "Type": row["property_type"],
        "Transaction": row["transaction_type"],
        "City": row["city"],
        "District": row["district"],
        "Price": f"{int(row['price']):,} SAR" if pd.notna(row["price"]) else "N/A",
        "Area": f"{int(row['area'])} sqm" if pd.notna(row["area"]) else "N/A",
        "Bedrooms": int(row["bedrooms"]) if pd.notna(row["bedrooms"]) else "N/A",
        "Bathrooms": int(row["bathrooms"]) if pd.notna(row["bathrooms"]) else "N/A",
        "Living Rooms": int(row["livings"]) if pd.notna(row["livings"]) else "N/A",
        "Furnished": row["has_furnished"],
        "Air Conditioning": row["has_air_condition"],
        "Kitchen": row["has_kitchen"],
        "Age": f"{int(row['age'])} years" if pd.notna(row["age"]) else "N/A",
        "Rent Period": row["rent_period"],
        "Advertiser": row["user_name"] if pd.notna(row["user_name"]) else "N/A",
        "Verified": row["is_verified"],
    }

    return "\n".join(f"{k}: {v}" for k, v in details.items())


@tool
def get_available_options() -> str:
    """Get the available filter options (cities, property types, etc.).

    Use this tool at the start of a conversation or when the customer is
    unsure what options are available. Helps guide them toward valid choices.

    Returns:
        A summary of available cities, property types, and price ranges.
    """
    cities = get_unique_cities()
    prop_types = get_unique_property_types()
    sell_range = get_price_range(transaction_type="Sell")
    rent_range = get_price_range(transaction_type="Rental")

    return (
        f"Available Cities: {', '.join(cities)}\n\n"
        f"Property Types: {', '.join(prop_types)}\n\n"
        f"Price Range (Buy): {sell_range['min']:,} - {sell_range['max']:,} SAR "
        f"(median: {sell_range['median']:,} SAR)\n"
        f"Price Range (Rent): {rent_range['min']:,} - {rent_range['max']:,} SAR "
        f"(median: {rent_range['median']:,} SAR)"
    )


ALL_TOOLS = [search_properties, get_property_details, get_available_options]
