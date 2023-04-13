import wbgapi as wb
import pandas as pd
import datetime
import requests as r
import io
import numpy as np


##### WORLD BANK DATA #####

NEIGHBORING_COUNTRIES = [i for i in wb.economy.list() if i['region'] == 'LCN']
NEIGHBORING_COUNTRIES_IDS = [i['id'] for i in  NEIGHBORING_COUNTRIES]

INTERNET_INDICATORS = ['IT.CEL.SETS',
                       'IT.CEL.SETS.P2',
                       'IT.MLT.MAIN',
                       'IT.MLT.MAIN.P2',
                       'IT.NET.BBND',
                       'IT.NET.BBND.P2',
                       'IT.NET.SECR',
                       'IT.NET.SECR.P6',
                       'IT.NET.USER.ZS']

wb_it_indicators_data = []
for row in wb.data.fetch(INTERNET_INDICATORS, NEIGHBORING_COUNTRIES_IDS): # all years
    year = int(row['time'][2:])
    row['year'] = year
    row['datetime'] = datetime.date(year, 1, 1)
    row['datetime_str'] = datetime.date(year, 1,1).strftime('%Y-%m-%d')
    wb_it_indicators_data.append(row)

df_wb_it = pd.DataFrame(wb_it_indicators_data)


##### DATOS ARG #####

URL_DATOS_INTERNET_SPEED = 'http://api.datosabiertos.enacom.gob.ar/api/v2/datastreams/VELOC-PROME-DE-BAJAD-51733/data.csv?auth_key=44a38fbffd39c9f7d84e8e7dd2e1d02f0950e611&download=1'
URL_DATOS_INTERNET_SPEED_STATE = 'http://api.datosabiertos.enacom.gob.ar/api/v2/datastreams/VELOC-PROME-DE-BAJAD-DE/data.csv?auth_key=44a38fbffd39c9f7d84e8e7dd2e1d02f0950e611&download=1'
URL_DATOS_INTERNET_ACCESS_BY_TYPE = 'http://api.datosabiertos.enacom.gob.ar/api/v2/datastreams/TOTAL-NACIO-DE-ACCES-A/data.csv?auth_key=44a38fbffd39c9f7d84e8e7dd2e1d02f0950e611&download=1'
URL_DATOS_INCOME_FIXED_INTERNET = 'http://api.datosabiertos.enacom.gob.ar/api/v2/datastreams/INGRE-POR-LA-OPERA-DEL/data.csv?auth_key=44a38fbffd39c9f7d84e8e7dd2e1d02f0950e611&download=1'
URL_DATOS_PENETRATION_FIXED_INTERNET = 'http://api.datosabiertos.enacom.gob.ar/api/v2/datastreams/PENET-DEL-INTER-FIJO-POR/data.csv?auth_key=44a38fbffd39c9f7d84e8e7dd2e1d02f0950e611&download=1'
URL_DATOS_PENETRATION_N_FIXED_INTERNET = 'http://api.datosabiertos.enacom.gob.ar/api/v2/datastreams/PENET-NACIO-DEL-INTER-FIJO/data.csv?auth_key=44a38fbffd39c9f7d84e8e7dd2e1d02f0950e611&download=1'
URL_DATOS_CONNECTIVITY = 'http://api.datosabiertos.enacom.gob.ar/api/v2/datastreams/CONEC-AL-SERVI-DE-INTER/data.csv?auth_key=44a38fbffd39c9f7d84e8e7dd2e1d02f0950e611&download=1'
URL_DATOS_WIFI_SPOTS = 'https://servicios.puntodigital.paisdigital.modernizacion.gob.ar/ws/ws-conectividadwifi.php?method=getCsv'




req_datos_internet_speed = r.get(URL_DATOS_INTERNET_SPEED).content.decode('utf-8')
dfInternetSpeed = pd.read_csv(io.StringIO(req_datos_internet_speed))
dfInternetSpeed = dfInternetSpeed.rename(columns={'Año': 'year', 'Mbps (Media de bajada)': 'value'})
dfInternetSpeed['value'] = dfInternetSpeed['value'].apply(lambda x: x.replace(',','.')).astype({'value': float})
dfInternetSpeed = dfInternetSpeed.groupby(['year']).agg({'value': np.max})
dfInternetSpeed['economy'] = 'ARG'
dfInternetSpeed['series'] = 'VELOC-PROME-DE-BAJAD'
dfInternetSpeed.reset_index(inplace=True)
dfInternetSpeed['state'] = np.nan
dfInternetSpeed['sub_series'] = np.nan



req_datos_internet_speed_state = r.get(URL_DATOS_INTERNET_SPEED_STATE).content.decode('utf-8')
dfInternetSpeedState = pd.read_csv(io.StringIO(req_datos_internet_speed_state))
dfInternetSpeedState = dfInternetSpeedState.rename(columns={'Año': 'year', 'Mbps (Media de bajada)': 'value', 'Provincia': 'state'})
dfInternetSpeedState = dfInternetSpeedState.groupby(['year', 'state']).agg({'value': np.max}).reset_index()
dfInternetSpeedState['economy'] = 'ARG'
dfInternetSpeedState['series'] = 'VELOC-PROME-DE-BAJAD-PROV'
dfInternetSpeedState['sub_series'] = np.nan



req_datos_income_fixed_internet = r.get(URL_DATOS_INCOME_FIXED_INTERNET).content.decode('utf-8')
dfIncomeFixedInternet = pd.read_csv(io.StringIO(req_datos_income_fixed_internet))
dfIncomeFixedInternet = dfIncomeFixedInternet.rename(columns={'Año': 'year', 'Ingresos (miles de pesos)': 'value'})
dfIncomeFixedInternet['value'] = dfIncomeFixedInternet['value'].apply(lambda x: x.replace('.','')).astype({'value': float})
dfIncomeFixedInternet = dfIncomeFixedInternet.groupby(['year']).agg({'value': np.max})
dfIncomeFixedInternet.reset_index(inplace=True)
dfIncomeFixedInternet['value'] = dfIncomeFixedInternet['value'].astype({'value': int})
dfIncomeFixedInternet['economy'] = 'ARG'
dfIncomeFixedInternet['series'] = 'INGRE-POR-LA-OPERA-DEL'
dfIncomeFixedInternet['state'] = np.nan
dfIncomeFixedInternet['sub_series'] = np.nan



req_datos_penetration_fixed_internet = r.get(URL_DATOS_PENETRATION_FIXED_INTERNET).content.decode('utf-8')
dfPenetrationFixedInternet = pd.read_csv(io.StringIO(req_datos_penetration_fixed_internet))
dfPenetrationFixedInternet = dfPenetrationFixedInternet.rename(columns={'Año': 'year', 'Accesos por cada 100 hab': 'value', 'Provincia': 'state'})
dfPenetrationFixedInternet = dfPenetrationFixedInternet.groupby(['year', 'state']).agg({'value': np.max})
dfPenetrationFixedInternet.reset_index(inplace=True)
dfPenetrationFixedInternet['economy'] = 'ARG'
dfPenetrationFixedInternet['series'] = 'PENET-DEL-INTER-FIJO-POR_PROV'
dfPenetrationFixedInternet['sub_series'] = np.nan



req_datos_penetration_n_fixed_internet = r.get(URL_DATOS_PENETRATION_N_FIXED_INTERNET).content.decode('utf-8')
dfPenetrationNacionalFixedInternet = pd.read_csv(io.StringIO(req_datos_penetration_n_fixed_internet))
dfPenetrationNacionalFixedInternet = dfPenetrationNacionalFixedInternet.groupby(['Año']).agg({'Accesos por cada 100 hogares': np.max, 'Accesos por cada 100 hab': np.max}).reset_index()
dfPenetrationNacionalFixedInternetHomes = dfPenetrationNacionalFixedInternet[['Año', 'Accesos por cada 100 hogares']]
dfPenetrationNacionalFixedInternetHomes.rename(columns={'Año': 'year', 'Accesos por cada 100 hogares': 'value'}, inplace=True)
dfPenetrationNacionalFixedInternetHomes['economy'] = 'ARG'
dfPenetrationNacionalFixedInternetHomes['series'] = 'PENET-NACIO-DEL-INTER-FIJO-NACIONAL-HOGAR'
dfPenetrationNacionalFixedInternetHomes['state'] = np.nan
dfPenetrationNacionalFixedInternetHomes['sub_series'] = np.nan



dfPenetrationNacionalFixedInternetPersons = dfPenetrationNacionalFixedInternet[['Año', 'Accesos por cada 100 hab']]
dfPenetrationNacionalFixedInternetPersons.rename(columns={'Año': 'year', 'Accesos por cada 100 hab': 'value'}, inplace=True)
dfPenetrationNacionalFixedInternetPersons['economy'] = 'ARG'
dfPenetrationNacionalFixedInternetPersons['series'] = 'PENET-NACIO-DEL-INTER-FIJO-NACIONAL-HAB'
dfPenetrationNacionalFixedInternetPersons['state'] = np.nan
dfPenetrationNacionalFixedInternetPersons['sub_series'] = np.nan



req_datos_internet_access_by_type = r.get(URL_DATOS_INTERNET_ACCESS_BY_TYPE).content.decode('utf-8')
dfAccessByType = pd.read_csv(io.StringIO(req_datos_internet_access_by_type))
dfAccessByType = dfAccessByType.rename(columns={'Año': 'year', 
                                                'ADSL': 'Acceso a Internet: ADSL',
                                                'Cablemodem': 'Acceso a Internet: Cablemodem',
                                                'Fibra óptica': 'Acceso a Internet: Fibra óptica',
                                                'Wireless': 'Acceso a Internet: Wireless',
                                                'Otros': 'Acceso a Internet: Otros',
                                                'Total': 'Acceso a Internet: Total Access Internet'
})
dfAccessByType = pd.melt(dfAccessByType, id_vars='year', value_vars=['Acceso a Internet: ADSL',
       'Acceso a Internet: Cablemodem', 'Acceso a Internet: Fibra óptica',
       'Acceso a Internet: Wireless', 'Acceso a Internet: Otros',
       'Acceso a Internet: Total Access Internet'], var_name='sub_series')
dfAccessByType['value'] = dfAccessByType['value'].astype({'value': str}).apply(lambda x: x.replace('.','')).astype({'value': int})
dfAccessByType = dfAccessByType.groupby(['year', 'sub_series']).agg({'value': np.sum}).reset_index()
dfAccessByType['economy'] = 'ARG'
dfAccessByType['series'] = 'TOTAL-NACIO-DE-ACCES'
dfAccessByType['state'] = np.nan




req_datos_wifi_spots = r.get(URL_DATOS_WIFI_SPOTS).content.decode('utf-8')
dfWifiSpots = pd.read_csv(io.StringIO(req_datos_wifi_spots))
dfWifiSpots.to_csv('wifi_spots.csv', index=False)
dfWifiSpots = dfWifiSpots.groupby(['provincia']).agg({'id': np.size}).reset_index()
dfWifiSpots = dfWifiSpots.rename(columns={'id': 'value', 'provincia': 'state'})
dfWifiSpots['year'] = 2022
dfWifiSpots['series'] = 'WIFI-SPOTS-PROVINCIA'
dfWifiSpots['economy'] = 'ARG'
dfWifiSpots['sub_series'] = np.nan



req_datos_conectivity = r.get(URL_DATOS_CONNECTIVITY).content.decode('utf-8')
dfConectivity = pd.read_csv(io.StringIO(req_datos_conectivity))
dfConectivity['economy'] = 'ARG'

dfDatos = pd.concat([dfInternetSpeed, dfInternetSpeedState, dfIncomeFixedInternet, dfPenetrationFixedInternet, dfPenetrationNacionalFixedInternetHomes, dfPenetrationNacionalFixedInternetPersons, dfAccessByType, dfWifiSpots])
dfDatos['source'] = 'ENACOM'



##### DATOS INDEC #####

URL_INDEC_INTERNET = 'https://www.indec.gob.ar/ftp/cuadros/sociedad/accesos_internet.xls'


req_indec_internet = r.get(URL_INDEC_INTERNET)
excel_indec_internet = pd.ExcelFile(req_indec_internet.content)


dfIndecInternetState = excel_indec_internet.parse('Cuadro 3', skiprows=4, skipfooter=2)
dfIndecInternetState.drop(columns=['Unnamed: 1'], inplace=True)

MONTHS = [1,2,3,4,5,6,7,8,9,10,11,12]
dfIndecInternetState2017 = dfIndecInternetState[dfIndecInternetState.columns[1:13]]
dfIndecInternetState2017.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetState2017['year'] = 2017
dfIndecInternetState2017['state'] = dfIndecInternetState[dfIndecInternetState.columns[0]]
dfIndecInternetState2017 = pd.melt(dfIndecInternetState2017, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()

dfIndecInternetState2018 = dfIndecInternetState[dfIndecInternetState.columns[13:25]]
dfIndecInternetState2018.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetState2018['year'] = 2018
dfIndecInternetState2018['state'] = dfIndecInternetState[dfIndecInternetState.columns[0]]
dfIndecInternetState2018 = pd.melt(dfIndecInternetState2018, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()

dfIndecInternetState2019 = dfIndecInternetState[dfIndecInternetState.columns[25:37]]
dfIndecInternetState2019['state'] = dfIndecInternetState[dfIndecInternetState.columns[0]]
dfIndecInternetState2019['year'] = 2019
dfIndecInternetState2019['state'] = dfIndecInternetState[dfIndecInternetState.columns[0]]
dfIndecInternetState2019 = pd.melt(dfIndecInternetState2019, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()

dfIndecInternetState2020 = dfIndecInternetState[dfIndecInternetState.columns[37:49]]
dfIndecInternetState2020['state'] = dfIndecInternetState[dfIndecInternetState.columns[0]]
dfIndecInternetState2020['year'] = 2020
dfIndecInternetState2020['state'] = dfIndecInternetState[dfIndecInternetState.columns[0]]
dfIndecInternetState2020 = pd.melt(dfIndecInternetState2020, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()

dfIndecInternetState2021 = dfIndecInternetState[dfIndecInternetState.columns[49:61]]
dfIndecInternetState2021['state'] = dfIndecInternetState[dfIndecInternetState.columns[0]]
dfIndecInternetState2021['year'] = 2021
dfIndecInternetState2021['state'] = dfIndecInternetState[dfIndecInternetState.columns[0]]
dfIndecInternetState2021 = pd.melt(dfIndecInternetState2021, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()

dfIndecInternetState2022 = dfIndecInternetState[dfIndecInternetState.columns[61:73]]
dfIndecInternetState2022['state'] = dfIndecInternetState[dfIndecInternetState.columns[0]]
dfIndecInternetState2022['year'] = 2022
dfIndecInternetState2022['state'] = dfIndecInternetState[dfIndecInternetState.columns[0]]
dfIndecInternetState2022 = pd.melt(dfIndecInternetState2022, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()

dfIndecInternetStateConcat = pd.concat([dfIndecInternetState2017, dfIndecInternetState2018, dfIndecInternetState2019, dfIndecInternetState2020, dfIndecInternetState2021, dfIndecInternetState2022])
dfIndecInternetStateConcat['series'] = 'INDEC-ACCESOS-INTERNET-PROVINCIA'
dfIndecInternetStateConcat['economy'] = 'ARG'
dfIndecInternetStateConcat['sub_series'] = np.nan




dfIndecInternetDevice = excel_indec_internet.parse('Cuadro 4', skiprows=5, skipfooter=2)
dfIndecInternetDevice.drop(columns=['Unnamed: 1'], inplace=True)
dfIndecInternetDevicePhone = dfIndecInternetDevice.iloc[:24]

dfIndecInternetDevicePhone2017 = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[1:13]]
dfIndecInternetDevicePhone2017.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDevicePhone2017['year'] = 2017
dfIndecInternetDevicePhone2017['state'] = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[0]]
dfIndecInternetDevicePhone2017 = pd.melt(dfIndecInternetDevicePhone2017, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDevicePhone2018 = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[13:25]]
dfIndecInternetDevicePhone2018.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDevicePhone2018['year'] = 2018
dfIndecInternetDevicePhone2018['state'] = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[0]]
dfIndecInternetDevicePhone2018 = pd.melt(dfIndecInternetDevicePhone2018, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDevicePhone2019 = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[25:37]]
dfIndecInternetDevicePhone2019.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDevicePhone2019['year'] = 2019
dfIndecInternetDevicePhone2019['state'] = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[0]]
dfIndecInternetDevicePhone2019 = pd.melt(dfIndecInternetDevicePhone2019, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDevicePhone2020 = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[37:49]]
dfIndecInternetDevicePhone2020.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDevicePhone2020['year'] = 2020
dfIndecInternetDevicePhone2020['state'] = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[0]]
dfIndecInternetDevicePhone2020 = pd.melt(dfIndecInternetDevicePhone2020, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDevicePhone2021 = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[49:61]]
dfIndecInternetDevicePhone2021.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDevicePhone2021['year'] = 2021
dfIndecInternetDevicePhone2021['state'] = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[0]]
dfIndecInternetDevicePhone2021 = pd.melt(dfIndecInternetDevicePhone2021, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDevicePhone2022 = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[61:73]]
dfIndecInternetDevicePhone2022.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDevicePhone2022['year'] = 2022
dfIndecInternetDevicePhone2022['state'] = dfIndecInternetDevicePhone[dfIndecInternetDevicePhone.columns[0]]
dfIndecInternetDevicePhone2022 = pd.melt(dfIndecInternetDevicePhone2022, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDevicePhoneConcat = pd.concat([dfIndecInternetDevicePhone2017, dfIndecInternetDevicePhone2018, dfIndecInternetDevicePhone2019, dfIndecInternetDevicePhone2020, dfIndecInternetDevicePhone2021, dfIndecInternetDevicePhone2022])
dfIndecInternetDevicePhoneConcat['series'] = 'INDEC-ACCESOS-INTERNET-DISPOSITIVO-FIJO'
dfIndecInternetDevicePhoneConcat['economy'] = 'ARG'
dfIndecInternetDevicePhoneConcat['sub_series'] = np.nan




dfIndecInternetDeviceMobile = dfIndecInternetDevice.iloc[30:]
dfIndecInternetDeviceMobile2017 = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[1:13]]
dfIndecInternetDeviceMobile2017.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDeviceMobile2017['year'] = 2017
dfIndecInternetDeviceMobile2017['state'] = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[0]]
dfIndecInternetDeviceMobile2017 = pd.melt(dfIndecInternetDeviceMobile2017, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDeviceMobile2018 = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[13:25]]
dfIndecInternetDeviceMobile2018.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDeviceMobile2018['year'] = 2018
dfIndecInternetDeviceMobile2018['state'] = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[0]]
dfIndecInternetDeviceMobile2018 = pd.melt(dfIndecInternetDeviceMobile2018, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDeviceMobile2019 = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[25:37]]
dfIndecInternetDeviceMobile2019.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDeviceMobile2019['year'] = 2019
dfIndecInternetDeviceMobile2019['state'] = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[0]]
dfIndecInternetDeviceMobile2019 = pd.melt(dfIndecInternetDeviceMobile2019, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDeviceMobile2020 = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[37:49]]
dfIndecInternetDeviceMobile2020.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDeviceMobile2020['year'] = 2020
dfIndecInternetDeviceMobile2020['state'] = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[0]]
dfIndecInternetDeviceMobile2020 = pd.melt(dfIndecInternetDeviceMobile2020, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDeviceMobile2021 = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[49:61]]
dfIndecInternetDeviceMobile2021.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDeviceMobile2021['year'] = 2021
dfIndecInternetDeviceMobile2021['state'] = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[0]]
dfIndecInternetDeviceMobile2021 = pd.melt(dfIndecInternetDeviceMobile2021, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDeviceMobile2022 = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[61:73]]
dfIndecInternetDeviceMobile2022.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetDeviceMobile2022['year'] = 2022
dfIndecInternetDeviceMobile2022['state'] = dfIndecInternetDeviceMobile[dfIndecInternetDeviceMobile.columns[0]]
dfIndecInternetDeviceMobile2022 = pd.melt(dfIndecInternetDeviceMobile2022, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetDeviceMobileConcat = pd.concat([dfIndecInternetDeviceMobile2017, dfIndecInternetDeviceMobile2018, dfIndecInternetDeviceMobile2019, dfIndecInternetDeviceMobile2020, dfIndecInternetDeviceMobile2021, dfIndecInternetDeviceMobile2022])
dfIndecInternetDeviceMobileConcat['series'] = 'INDEC-ACCESOS-INTERNET-DISPOSITIVO-MOBILE'
dfIndecInternetDeviceMobileConcat['economy'] = 'ARG'
dfIndecInternetDeviceMobileConcat['sub_series'] = np.nan





dfIndecInternetCategoria = excel_indec_internet.parse('Cuadro 5', skiprows=5, skipfooter=2)
dfIndecInternetCategoria.drop(columns=['Unnamed: 1'], inplace=True)
dfIndecInternetCategoriaResidential = dfIndecInternetCategoria.iloc[:24]


dfIndecInternetCategoriaResidential2017 = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[1:13]]
dfIndecInternetCategoriaResidential2017.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaResidential2017['year'] = 2017
dfIndecInternetCategoriaResidential2017['state'] = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[0]]
dfIndecInternetCategoriaResidential2017 = pd.melt(dfIndecInternetCategoriaResidential2017, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()



dfIndecInternetCategoriaResidential2018 = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[13:25]]
dfIndecInternetCategoriaResidential2018.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaResidential2018['year'] = 2018
dfIndecInternetCategoriaResidential2018['state'] = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[0]]
dfIndecInternetCategoriaResidential2018 = pd.melt(dfIndecInternetCategoriaResidential2018, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()



dfIndecInternetCategoriaResidential2019 = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[25:37]]
dfIndecInternetCategoriaResidential2019.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaResidential2019['year'] = 2019
dfIndecInternetCategoriaResidential2019['state'] = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[0]]
dfIndecInternetCategoriaResidential2019 = pd.melt(dfIndecInternetCategoriaResidential2019, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()



dfIndecInternetCategoriaResidential2020 = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[37:49]]
dfIndecInternetCategoriaResidential2020.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaResidential2020['year'] = 2020
dfIndecInternetCategoriaResidential2020['state'] = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[0]]
dfIndecInternetCategoriaResidential2020 = pd.melt(dfIndecInternetCategoriaResidential2020, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()



dfIndecInternetCategoriaResidential2021 = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[49:61]]
dfIndecInternetCategoriaResidential2021.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaResidential2021['year'] = 2021
dfIndecInternetCategoriaResidential2021['state'] = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[0]]
dfIndecInternetCategoriaResidential2021 = pd.melt(dfIndecInternetCategoriaResidential2021, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()



dfIndecInternetCategoriaResidential2022 = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[61:73]]
dfIndecInternetCategoriaResidential2022.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaResidential2022['year'] = 2022
dfIndecInternetCategoriaResidential2022['state'] = dfIndecInternetCategoriaResidential[dfIndecInternetCategoriaResidential.columns[0]]
dfIndecInternetCategoriaResidential2022 = pd.melt(dfIndecInternetCategoriaResidential2022, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetCategoriaResidentialConcat = pd.concat([dfIndecInternetCategoriaResidential2017, dfIndecInternetCategoriaResidential2018, dfIndecInternetCategoriaResidential2019, dfIndecInternetCategoriaResidential2020, dfIndecInternetCategoriaResidential2021, dfIndecInternetCategoriaResidential2022])
dfIndecInternetCategoriaResidentialConcat['series'] = 'INDEC-ACCESOS-INTERNET-CATEGORIA-RESIDENCIALES'
dfIndecInternetCategoriaResidentialConcat['economy'] = 'ARG'
dfIndecInternetCategoriaResidentialConcat['sub_series'] = np.nan


dfIndecInternetCategoriaBusiness = dfIndecInternetCategoria.iloc[30:]
dfIndecInternetCategoriaBusiness2017 = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[1:13]]
dfIndecInternetCategoriaBusiness2017.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaBusiness2017['year'] = 2017
dfIndecInternetCategoriaBusiness2017['state'] = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[0]]
dfIndecInternetCategoriaBusiness2017 = pd.melt(dfIndecInternetCategoriaBusiness2017, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()


dfIndecInternetCategoriaBusiness2018 = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[13:25]]
dfIndecInternetCategoriaBusiness2018.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaBusiness2018['year'] = 2018
dfIndecInternetCategoriaBusiness2018['state'] = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[0]]
dfIndecInternetCategoriaBusiness2018 = pd.melt(dfIndecInternetCategoriaBusiness2018, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()



dfIndecInternetCategoriaBusiness2019 = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[25:37]]
dfIndecInternetCategoriaBusiness2019.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaBusiness2019['year'] = 2019
dfIndecInternetCategoriaBusiness2019['state'] = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[0]]
dfIndecInternetCategoriaBusiness2019 = pd.melt(dfIndecInternetCategoriaBusiness2019, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()



dfIndecInternetCategoriaBusiness2020 = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[37:49]]
dfIndecInternetCategoriaBusiness2020.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaBusiness2020['year'] = 2020
dfIndecInternetCategoriaBusiness2020['state'] = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[0]]
dfIndecInternetCategoriaBusiness2020 = pd.melt(dfIndecInternetCategoriaBusiness2020, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()



dfIndecInternetCategoriaBusiness2021 = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[49:61]]
dfIndecInternetCategoriaBusiness2021.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaBusiness2021['year'] = 2021
dfIndecInternetCategoriaBusiness2021['state'] = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[0]]
dfIndecInternetCategoriaBusiness2021 = pd.melt(dfIndecInternetCategoriaBusiness2021, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()



dfIndecInternetCategoriaBusiness2022 = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[61:73]]
dfIndecInternetCategoriaBusiness2022.set_axis(MONTHS, axis=1, inplace=True)
dfIndecInternetCategoriaBusiness2022['year'] = 2022
dfIndecInternetCategoriaBusiness2022['state'] = dfIndecInternetCategoriaBusiness[dfIndecInternetCategoriaBusiness.columns[0]]
dfIndecInternetCategoriaBusiness2022 = pd.melt(dfIndecInternetCategoriaBusiness2022, id_vars=['year', 'state']).groupby(['year', 'state']).agg({'value': np.sum}).reset_index()

dfIndecInternetCategoriaBusinessConcat = pd.concat([dfIndecInternetCategoriaBusiness2017, dfIndecInternetCategoriaBusiness2018, dfIndecInternetCategoriaBusiness2019, dfIndecInternetCategoriaBusiness2020, dfIndecInternetCategoriaBusiness2021, dfIndecInternetCategoriaBusiness2022])
dfIndecInternetCategoriaBusinessConcat['series'] = 'INDEC-ACCESOS-INTERNET-CATEGORIA-ORGANIZACIONALES'
dfIndecInternetCategoriaBusinessConcat['economy'] = 'ARG'
dfIndecInternetCategoriaBusinessConcat['sub_series'] = np.nan





dfIndec = pd.concat([dfIndecInternetStateConcat, dfIndecInternetDevicePhoneConcat, dfIndecInternetDeviceMobileConcat, dfIndecInternetCategoriaResidentialConcat, dfIndecInternetCategoriaBusinessConcat])
dfIndec['state'] = dfIndec['state'].replace('CABA y provincia de Bs.As.', 'Buenos Aires')
dfIndec['state'] = dfIndec['state'].replace('Total del país', 'Total')
dfIndec['source'] = 'INDEC'



df_wb_it.to_csv('internet_argentina_wb.csv', index=False)
dfDatos.to_csv('internet_argentina_datos.csv', index=False)
dfConectivity.to_csv('internet_argentina_datos_conn.csv', index=False)
dfIndec.to_csv('internet_argentina_datos_indec.csv', index=False)



dfCountries = wb.economy.info(NEIGHBORING_COUNTRIES_IDS)
dfCountries = pd.DataFrame(dfCountries.table(), columns=dfCountries.columns)
dfCountries.iloc[:-1].to_csv('countries_ids.csv', index=False)
