import pandas as pd
from app.config import DATA_PATH

_df: pd.DataFrame | None = None


def _load() -> pd.DataFrame:
    global _df
    if _df is not None:
        return _df

    df = pd.read_csv(DATA_PATH)

    # Drop exact duplicate rows
    df = df.drop_duplicates(subset=["row_id"], keep="first")

    # Clean numeric columns
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["area"] = pd.to_numeric(df["area"], errors="coerce")
    df["bedrooms"] = pd.to_numeric(df["bedrooms"], errors="coerce")
    df["bathrooms"] = pd.to_numeric(df["bathrooms"], errors="coerce")
    df["livings"] = pd.to_numeric(df["livings"], errors="coerce")
    df["age"] = pd.to_numeric(df["age"], errors="coerce")

    # Normalize string columns
    for col in ["city", "district", "property_type", "transaction_type",
                 "has_furnished", "has_air_condition", "has_kitchen", "rent_period"]:
        df[col] = df[col].astype(str).str.strip()

    _df = df
    return _df


def get_dataframe() -> pd.DataFrame:
    return _load()


def get_unique_cities() -> list[str]:
    df = _load()
    return sorted(df["city"].dropna().unique().tolist())


def get_unique_property_types() -> list[str]:
    df = _load()
    return sorted(df["property_type"].dropna().unique().tolist())


def get_districts_for_city(city: str) -> list[str]:
    df = _load()
    mask = df["city"].str.contains(city, case=False, na=False)
    return sorted(df.loc[mask, "district"].dropna().unique().tolist())


def get_price_range(property_type: str | None = None,
                    transaction_type: str | None = None) -> dict:
    df = _load()
    mask = pd.Series(True, index=df.index)
    if property_type:
        mask &= df["property_type"].str.lower() == property_type.lower()
    if transaction_type:
        mask &= df["transaction_type"].str.lower() == transaction_type.lower()
    prices = df.loc[mask, "price"].dropna()
    if prices.empty:
        return {"min": 0, "max": 0, "median": 0}
    return {
        "min": int(prices.min()),
        "max": int(prices.max()),
        "median": int(prices.median()),
    }
