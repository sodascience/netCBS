import polars as pl
from numpy import random
from pathlib import Path

context2path = {
    "Family": "FAMILIENETWERKTAB",
    "Colleagues": "COLLEGANETWERKTAB",
    "Neighbors": "BURENNETWERKTAB",
    "Schoolmates": "KLASGENOTENNETWERKTAB",
    "Housemates": "HUISGENOTENNETWERKTAB"
}

context2types = {
    "Family": ["301", "302", "303", "304", "305", "306", "307", "308", "309", "310", "311", 
               "312", "313", "314", "315", "316", "317", "318", "319", "320", "321", "322"],
    "Colleagues": ["201"],
    "Neighbors": ["101", "102"],
    "Schoolmates": ["501", "502", "503", "504", "505", "506"],
    "Housemates": ["401", "402"]
}

# Create synthetic data for each type of context
def create_synthetic_data(context, year, N, v=1, outpath="cbsdata/Bevolking"):
    """
    Create synthetic data for a given context and year

    Parameters
    ----------
    context : str
        The context for which to create synthetic data
    year : int
        The year for which to create synthetic data
    N : int
        The number of rows to create
    v : int
        The version of the data to create

    Returns
    -------
    None

    """
    path = context2path[context]
    outpath = f"{outpath}/{path}/{path}{year}V{v}.csv"
    # create folder if it does not exist using Pathlib
    Path(outpath).parent.mkdir(parents=True, exist_ok=True)
    set_types = context2types[context]

    # Create a dataframe with 3 columns, RINPERSOON, RINPERSOONS (R, S) and RELATIE
    N_extra = int(N*1.2)
    RP_ego = random.choice(range(100_000_000, 100_500_000), N_extra, replace=True)
    RP_alter = random.choice(range(100_000_000, 100_500_000), N_extra, replace=True)
    relatie = random.choice(set_types, N_extra, replace=True)
    df = pl.DataFrame({"RINPERSOON": RP_ego, "RINPERSOONS":  ["R"]*N_extra,
                       "RINPERSOONRELATIE": RP_alter, "RINPERSOONSRELATIE":  ["R"]*N_extra,
                       "RELATIE": relatie}
                       )
    
    # Drop duplicates
    df = df.unique().head(N)

    # Save the dataframe to a CSV file
    df.write_csv(outpath)


if __name__ == "__main__":
    # Add a CLI to this script
    create_synthetic_data("Family", 2021, 1_000_000, outpath="cbsdata/Bevolking")
    create_synthetic_data("Colleagues", 2021, 1_000_000, outpath="cbsdata/Bevolking")
    create_synthetic_data("Neighbors", 2021, 1_000_000, outpath="cbsdata/Bevolking")
    create_synthetic_data("Schoolmates", 2021, 1_000_000, outpath="cbsdata/Bevolking")
    create_synthetic_data("Housemates", 2021, 1_000_000, outpath="cbsdata/Bevolking")
