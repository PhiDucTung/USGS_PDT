# Discretized US Drought Data to Support Statistical Modeling

Drought is a costly and disruptive natural disaster, with widespread implications for agriculture, wildfire, and urban planning. We present a novel data set on US drought built to enable computationally efficient spatio-temporal statistical and probabilistic models of drought. We converted drought data obtained from the widely-used US Drought Monitor (USDM) from continuous shape files to a 0.5 degree regular lattice. These data cover the Continental US from 2000 to mid-2022. Known environmental drivers of drought include variables obtained from the North American Land Data Assimilation System (NLDAS-2), US Geological Survey (USGS) streamflow data, and National Oceanic and Atmospheric Administration (NOAA) teleconnections data. The spatially varying variables have been processed to represent weekly averages on the common lattice that is used for drought. The USGS streamflow data is itself a new gridded data product, aggregating point-referenced stream discharges from across the US to the common lattice using watersheds to combine nearby stream data. The resulting data set permits statistical and probabilistic modeling of drought with explicit spatial and/or temporal dependence.

## Description of the data and file structure

The dataset is saved as a CSV file. Each row in the spreadsheet corresponds to a grid cell for a particular week. The columns correspond to the variables which are described below.

Index variables:

*   'time' is YYYYMMDD
*   'grid' is an arbitrary naming convention based on lat/lon. Letters index lat, and numbers index lon. See RasterizeUSDMForDryad.R for details.
*   'lon' is the longitude of the centroid for the grid cell
*   'lat' is the latitude for the centroid for the grid cell

USDM variable:

*   'drought' is a factor with levels 0, D0, D1, D2, D3, D4 and refer to the value of the USDM at the particular time and at the grid centroid.

NLDAS-2 processed variables:

*   APCP: weekly total precipitation [kg/m2], originally from Forcing File A
*   EVP:  weekly average evapotranspiration [kg/m2], originally from NOAH Land Surface Model
*   LAI: weekly average leaf area index [-], originally from NOAH Land Surface Model
*   PEVAP: weekly average potential evaporation [kg/m2], originally from Forcing File A
*   PEVPR: weekly average potential latent heat flux [W/m2], originally from NOAH Land Surface Model
*   SOILM: weekly average soil moisture content [kg/m2], originally from NOAH Land Surface Model
*   SNOD: weekly average snow depth [m], originally from NOAH Land Surface Model
*   SNOM: weekly average snow melt [kg/m2], originally from NOAH Land Surface Model
*   SNOWC: weekly average snow cover fraction [-], orginally from NOAH Land Surface Model
*   SSRUN: weekly average surface runoff [kg/m2], originally from NOAH Land Surface Model
*   TSOIL: weekly average soil temperature [K], originally from NOAH Land Surface Model
*   WEASD: weekly average water equivalent of accumulated snow depth [kg/m2], originally from NOAH Land Surface Model

Streamflow variables:
For each in-situ stream gauge, 30-year empirical distributions of average 7-day, 14-day, and 28-day streamflows were computed, and all individual values are expressed as averages of percentiles of the corresponding empirical distribution. The three variables below show mean percentiles for teach time/grid over 7, 14, and 28 days. For any combination where no gauges are present within a grid cell, an NA appears.

*   'percFlow7day'
*   'percFlow14day'
*   'percFlow28day'

To infill missing data, averages of the same data are taken over watersheds as defined by hydrologic unit code (HUC) of increasing sizes needed to infill data. HUC 8 is the smallest watershed, and HUC6 and HUC4 are larger. These averages can be either unewighted (e.g. avg.HUC.7day), or weighted by inverse distance between each gauge and the grid cell centroid (e.g. avgDist.HUC8.7day). The following are all averages taken of data from different lengths (7, 14, 28), HUC (8, 6, 4) and weighting (none of avgDist).

*   'avg.HUC8.7day'
*   'avgDist.HUC8.7day'
*   'avg.HUC6.7day'
*   'avgDist.HUC6.7day'
*   'avg.HUC4.7day'
*   'avgDist.HUC4.7day'
*   'avg.HUC8.14day'
*   'avgDist.HUC8.14day'
*   'avg.HUC6.14day'
*   'avgDist.HUC6.14day'
*   'avg.HUC4.14day'
*   'avgDist.HUC4.14day'
*   'avg.HUC8.28day'
*   'avgDist.HUC8.28day'
*   'avg.HUC6.28day'
*   'avgDist.HUC6.28day'
*   'avg.HUC4.28day'
*   'avgDist.HUC4.28day'

Teleconnections variables:

*   'pna' is the weekly average of the Pacific/North American Pattern
*   'nao' is the weekly average of the North Atlantic Oscillation
*   'ao' is the weekly average of the Arctic Oscillation
*   'enso' is the weekly average of El Nino 3.4 index
*   'enso14' is the 14-day average of El Nino 3.4 index
*   'enso28' is the 28-day average of El Nino 3.4 index
*   'enso84' is the 84-day average of El Nino 3.4 index

## Sharing/Access information

All raw data are publicly and freely available at the following:

*   [https://droughtmonitor.unl.edu/DmData/GISData.aspx](https://droughtmonitor.unl.edu/DmData/GISData.aspx)
*   [https://disc.gsfc.nasa.gov/datasets?keywords=NLDAS](https://disc.gsfc.nasa.gov/datasets?keywords=NLDAS)
*   [https://waterdata.usgs.gov/nwis](https://waterdata.usgs.gov/nwis)
*   [https://www.cpc.ncep.noaa.gov/data/indices/wksst9120.for](https://www.cpc.ncep.noaa.gov/data/indices/wksst9120.for)
*   [https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.pna.index.b500101.current.asci](https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.pna.index.b500101.current.asci)
*   [https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.nao.index.b500101.current.ascii](https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.nao.index.b500101.current.ascii)
*   [https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.ao.index.b500101.current.ascii](https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.ao.index.b500101.current.ascii)

## Code/Software

All code files needed to process the raw data to output this data set can be found on [https://github.com/heplersa/USDMdata](https://github.com/heplersa/USDMdata)
