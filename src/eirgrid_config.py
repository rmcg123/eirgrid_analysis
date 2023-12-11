"""Configuration settings for Eirgrid analysis."""

DATA_FOLDER = "data/"
RESULTS_FOLDER = "results/"

EIRGRID_API_URL = "https://www.smartgriddashboard.com/DashboardService.svc/data"

DATA_AREAS = {
    "wind_generation": "windactual",
    "co2_intensity": "co2intensity",
    "snsp": "SnspALL",
    "demand": "demandactual",
    "generation": "generationactual",
    "interconnection": "interconnection",
}
START_DATE_NAME = "datefrom"
END_DATE_NAME = "dateto"

REGION = "ALL"

YEARS = ["2023", "2022", "2021", "2020", "2019"]