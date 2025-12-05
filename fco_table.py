import pandas as pd

fco = pd.read_csv("/mnt/c/Users/alan.fontoura/Desktop/Current/Ownership.csv")


def ownership_of_ownership(df: pd.DataFrame) -> pd.DataFrame:
    own_of_own = df.merge(
        df, left_on="Owned", right_on="Owner", suffixes=("_1", "_2"), how="inner"
    )
    own_of_own["Date"] = own_of_own[["Date_1", "Date_2"]].max(axis=1)
    own_of_own["Ownership"] = own_of_own["Ownership_1"] * own_of_own["Ownership_2"]
    own_of_own = own_of_own[["Owner_1", "Owned_2", "Date", "Ownership"]]
    own_of_own.columns = ["Owner", "Owned", "Date", "Ownership"]
    return own_of_own.drop_duplicates().reset_index(drop=True)


def add_next_level(df: pd.DataFrame) -> pd.DataFrame:
    next_level = ownership_of_ownership(fco)
    combined = pd.concat([df, next_level]).drop_duplicates().reset_index(drop=True)
    return combined


def main(df: pd.DataFrame) -> pd.DataFrame:
    keep_going = True
    while keep_going:
        new_df = add_next_level(df)
        if len(new_df) == len(df):
            keep_going = False
        df = new_df
    df = df.sort_values(by=["Owned", "Date", "Owner"]).reset_index(drop=True)
    return df


if __name__ == "__main__":
    result = main(fco)
    result.to_csv(
        "/mnt/c/Users/alan.fontoura/Desktop/Current/Full_Ownership.csv", index=False
    )
