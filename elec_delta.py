# -*- coding: utf-8 -*-
"""
Created on Tue Jun 19 08:30:00 2018

@author: KLD300
"""

import pandas as pd

CONNECTION_GIS = "SERVER=PWGISSQL01;DATABASE=PCOGIS;"
CONNECTION_LVJDE = "SERVER=PWJDESQL01;DATABASE=JDE_PRODUCTION;"

ELEC_FILE = r"Electricity GIS-SAP Attribute Mapping.xlsx"
ELEC_SHEET1 = "GIS Attribute Initial Selection"
ELEC_SHEET2 = "Electricity Characteristics"


def file_loading():
    """Creats four data frames from three files."""
    print("Loading the three excel files.")

    elec_data1 = pd.read_excel(ELEC_FILE, sheet_name=ELEC_SHEET1)
    elec_data2 = pd.read_excel(ELEC_FILE, sheet_name=ELEC_SHEET2,
                               skiprows=1)

    strp_elec1 = elec_data1[['Table', 'Column', 'SAP required']]
    strp_elec1 = strp_elec1.rename(columns={'Table': 'TABLE',
                                            'Column': 'COLUMN',
                                            'SAP required': 'SAP'})

    strp_elec2 = elec_data2[['Source Table', 'Source Column', 'SAP required']]
    strp_elec2 = strp_elec2.rename(columns={'Source Table': 'TABLE',
                                            'Source Column': 'COLUMN',
                                            'SAP required': 'SAP'})
    return strp_elec1, strp_elec2


def old_strip_sql(data, sap_stat=True):
    """Removes non-existent and corrects tablenames."""
    tab_fixs = [["PCOGIS.SDE.", ''],
                ["Auxiliary Equipment", "AUXILLARYEQUIPMENT"]]
    for old_str, new_str in tab_fixs:
        data['TABLE'] = data['TABLE'].str.replace(old_str, new_str)
    data = data.dropna(subset=['COLUMN'])
    bad_atts = [" ", "SHAPE_Length", "HVFUSES", "LVFUSES", "SHAPE_Area",
                "ACTUALLENGTH", "DECOMMISSIONINGDATE", "DECOMMISSIONINGREASON"]
    data = data[~data['COLUMN'].isin(bad_atts)]
    bad_tab_atts = [['SWITCHUNIT$', 'INTERRUPTINGMEDIUM$'],
                    ['DistributionMain$', '^CROSSINGID$'],
                    ['DistributionMain$', '^MOUNTINGTYPE$'],
                    ['DistributionMain$', '^MOUNTINGPOSITION$']]
    for tab_str, att_str in bad_tab_atts:
        data = data[~(data['TABLE'].str.match(tab_str) &
                      data['COLUMN'].str.match(att_str))]
    bad_doubles = [['Regulator$', '^SUBTYPECD$', 'y'],
                   ['RegulatorStation$', '^EQUIPMENTID$', 'N'],
                   ['SurfaceStructure$', '^APPLICATION$', 'N'],
                   ['SurfaceStructure$', '^ENTRY$', 'N'],
                   ['SurfaceStructure$', '^FACILITYID$', 'N'],
                   ['SurfaceStructure$', '^MANUFACTURER$', 'N'],
                   ['SurfaceStructure$', '^MATERIAL$', 'N'],
                   ['SurfaceStructure$', '^MODEL$', 'N'],
                   ['SurfaceStructure$', '^STRUCTURESIZE$', 'N'],
                   ['COMMSPOWERSUPPLY$', '^BATTERYAMPERAGEHOURS$', 'N'],
                   ['COMMSPOWERSUPPLY$', '^BATTERYCOUNT$', 'N']]
    if sap_stat is True:
        for tab_str, att_str, sap_str in bad_doubles:
            data = data[~(data['TABLE'].str.match(tab_str) &
                          data['COLUMN'].str.match(att_str) &
                          data['SAP'].str.match(sap_str))]
        bad_null = [['SurfaceStructure$', '^ENCLOSURE$'],
                    ['SurfaceStructure$', '^ENCLOSURETYPE$'],
                    ['SurfaceStructure$', '^ENCLOSUREMANUFACTURER$']]
        for tab_str, att_str in bad_null:
            data = data[~(data['TABLE'].str.match(tab_str) &
                          data['COLUMN'].str.match(att_str) &
                          data['SAP'].isnull())]
    return data


def strip_sql(data, sap_stat=True):
    """Removes non-existent and corrects tablenames."""
    tab_fixs = [["PCOGIS.SDE.", ''],
                ["Auxiliary Equipment", "AUXILLARYEQUIPMENT"]]
    for old_str, new_str in tab_fixs:
        data['TABLE'] = data['TABLE'].str.replace(old_str, new_str)
    data = data.dropna(subset=['COLUMN'])
    bad_atts = [" ", "SHAPE_Length", "HVFUSES", "LVFUSES", "SHAPE_Area",
                "None", "ACTUALLENGTH", "DECOMMISSIONINGDATE",
                "DECOMMISSIONINGREASON", 'LOTS YET TO ADD']
    data = data[~data['COLUMN'].isin(bad_atts)]
    bad_tabs = ['LocationAttributes', 'CustomerConnections', 'TBD']
    data = data[~data['TABLE'].isin(bad_tabs)]
    bad_tab_atts = [['SWITCHUNIT$', 'INTERRUPTINGMEDIUM$'],
                    ['DistributionMain$', 'CROSSINGID$'],
                    ['DistributionMain$', 'MOUNTINGTYPE$'],
                    ['DistributionMain$', 'MOUNTINGPOSITION$']]
    for tab_str, att_str in bad_tab_atts:
        data = data[~(data['TABLE'].str.match(tab_str) &
                      data['COLUMN'].str.match(att_str))]
    bad_doubles = [['Regulator$', 'SUBTYPECD$', 'y'],
                   ['RegulatorStation$', 'EQUIPMENTID$', 'N'],
                   ['SurfaceStructure$', 'APPLICATION$', 'N'],
                   ['SurfaceStructure$', 'ENTRY$', 'N'],
                   ['SurfaceStructure$', 'FACILITYID$', 'N'],
                   ['SurfaceStructure$', 'MANUFACTURER$', 'N'],
                   ['SurfaceStructure$', 'MATERIAL$', 'N'],
                   ['SurfaceStructure$', 'MODEL$', 'N'],
                   ['SurfaceStructure$', 'STRUCTURESIZE$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'BATTERYAMPERAGEHOURS$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'BATTERYCOUNT$', 'N'],
                   ['PillarPoint$', 'DATEMANUFACTURED$', 'TBC'],
                   ['PillarPoint$', 'FACILITYID$', 'TBC'],
                   ['PillarPoint$', 'FEEDERID$', 'TBC'],
                   ['PillarPoint$', 'NUMBEROFUSEDCIRCUITS$', 'TBC'],
                   ['PillarPoint$', 'SUBTYPECD$', 'N'],
                   ['PillarPoint$', 'TOTALNUMBEROFCIRCUITS$', 'TBC'],
                   ['PillarPoint$', 'TRUENZMGPOS$', 'N'],
                   ['SupportStructure$', 'HIGHESTVOLTAGE$', 'N'],
                   ['SurfaceStructure$', 'ASSETFUNCTION$', 'N'],
                   ['SurfaceStructure$', 'ENCLOSUREMANUFACTURER$', 'N'],
                   ['SurfaceStructure$', 'ENCLOSURETYPE$', 'N'],
                   ['SurfaceStructure$', 'GLOBALID$', 'N'],
                   ['SurfaceStructure$', 'STREETNAME$', 'N'],
                   ['SurfaceStructure$', 'STREETNO$', 'N'],
                   ['SurfaceStructure$', 'SUBURB$', 'N'],
                   ['SurfaceStructure$', 'SYMBOLROTATION$', 'N'],
                   ['SurfaceStructure$', 'TOWN$', 'N'],
                   ['Switch$', 'FACILITYID$', 'N'],
                   ['Switch$', 'FEEDERID$', 'N'],
                   ['Switch$', 'FEEDERID2$', 'N'],
                   ['Switch$', 'GEONETFEEDERCODE$', 'N'],
                   ['Switch$', 'GLOBALID$', 'N'],
                   ['Switch$', 'GROUNDEDINDICATOR$', 'N'],
                   ['Switch$', 'INSTALLATIONDATE$', 'N'],
                   ['Switch$', 'MOUNTING$', 'N'],
                   ['Switch$', 'NORMALPOSITION$', 'N'],
                   ['Switch$', 'NUMPHASES$', 'N'],
                   ['Switch$', 'OPERATINGVOLTAGE$', 'N'],
                   ['Switch$', 'OUTOFORDERINDICATOR$', 'N'],
                   ['Switch$', 'REFERENCE$', 'N'],
                   ['Switch$', 'REMOTECONTROLLED$', 'N'],
                   ['Switch$', 'REMOTEINDICATION$', 'N'],
                   ['Switch$', 'RETICULATION$', 'N'],
                   ['Switch$', 'SITEID$', 'N'],
                   ['Switch$', 'STREETNAME$', 'N'],
                   ['Switch$', 'STREETNO$', 'N'],
                   ['Switch$', 'SUBTYPECD$', 'N'],
                   ['Switch$', 'SUBURB$', 'N'],
                   ['Switch$', 'SYMBOLROTATION$', 'N'],
                   ['Switch$', 'TOWN$', 'N'],
                   ['Switch$', 'WORKORDERID$', 'N'],
                   ['SWITCHUNIT$', 'ARCQUENCHING$', 'N'],
                   ['SWITCHUNIT$', 'C_INTJDEID$', 'N'],
                   ['SWITCHUNIT$', 'COMMENTS$', 'N'],
                   ['SWITCHUNIT$', 'DATEMANUFACTURED$', 'N'],
                   ['SWITCHUNIT$', 'DATEPURCHASED$', 'N'],
                   ['SWITCHUNIT$', 'INSTALLATIONDATE$', 'N'],
                   ['SWITCHUNIT$', 'INSULATIONMEDIUM$', 'N'],
                   ['SWITCHUNIT$', 'LOADBREAKINGCAPACITY$', 'N'],
                   ['SWITCHUNIT$', 'MANUFACTURER$', 'N'],
                   ['SWITCHUNIT$', 'MODEL$', 'N'],
                   ['SWITCHUNIT$', 'NORMALCURRENTRATING$', 'N'],
                   ['SWITCHUNIT$', 'NUMPHASES$', 'N'],
                   ['SWITCHUNIT$', 'OWNER$', 'N'],
                   ['SWITCHUNIT$', 'REFERENCE$', 'N'],
                   ['SWITCHUNIT$', 'SERIALNUMBER$', 'N'],
                   ['SWITCHUNIT$', 'VISUALEARTHINDICATOR$', 'N'],
                   ['SWITCHUNIT$', 'VOLTAGERATING$', 'N'],
                   ['SWITCHUNIT$', 'WORKORDERID$', 'N'],
                   ['UndergroundStructure$', 'C_INTJDEID$', 'N'],
                   ['UndergroundStructure$', 'COMMENTS$', 'N'],
                   ['UndergroundStructure$', 'FACILITYID$', 'N'],
                   ['UndergroundStructure$', 'FEEDERID$', 'N'],
                   ['UndergroundStructure$', 'GLOBALID$', 'N'],
                   ['UndergroundStructure$', 'HIGHESTVOLTAGE$', 'N'],
                   ['UndergroundStructure$', 'INSTALLATIONDATE$', 'N'],
                   ['UndergroundStructure$', 'OUTOFORDERINDICATOR$', 'N'],
                   ['UndergroundStructure$', 'OWNER$', 'N'],
                   ['UndergroundStructure$', 'REFERENCE$', 'N'],
                   ['UndergroundStructure$', 'STREETNAME$', 'N'],
                   ['UndergroundStructure$', 'STREETNO$', 'N'],
                   ['UndergroundStructure$', 'SUBURB$', 'N'],
                   ['UndergroundStructure$', 'SYMBOLROTATION$', 'N'],
                   ['UndergroundStructure$', 'TOWN$', 'N'],
                   ['UndergroundStructure$', 'WORKORDERID$', 'N'],
                   ['Fuse$', 'INSTALLATIONDATE$', 'N'],
                   ['Ground$', 'BELOWGROUNDCONNECTION$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'COOLINGTYPE$', 'TBD'],
                   ['POWERTRANSFORMERUNIT$', 'COOLINGTYPE2$', 'TBD'],
                   ['POWERTRANSFORMERUNIT$', 'COOLINGTYPE3$', 'TBD'],
                   ['POWERTRANSFORMERUNIT$', 'CTBURDENVA$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'CTCLASS$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'CTQUANTITY$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'CTRATIO$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'IMPEDANCE2$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'IMPEDANCE3$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'IMPEDANCEZ0$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'RATEDMVA$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'RATEDMVA2$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'RATEDMVA3$', 'N'],
                   ['AUXILLARYEQUIPMENT$', 'MANUFACTURER$', 'N'],
                   ['AUXILLARYEQUIPMENT$', 'MODEL$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'BATTERYTYPE$', 'N'],
                   ['SupportStructure$', 'FUNCTION_$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'GENERATORFUELTYPE$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'HOURSOFSUPPLY$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'PARALELLCOUNT$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'PARALELLCOUNT$', 'TBD'],
                   ['COMMSPOWERSUPPLY$', 'SYSTEMVOLTAGE$', 'TBD'],
                   ['SurfaceStructure$', 'TRUENZMGPOS$', 'N'],
                   ['SupportStructure$', 'ABSOLUTE$', 'N'],
                   ['DISTTRANSFUSEUNIT$', 'VOLTAGERATING$', 'N'],
                   ['DISTTRANSFUSEUNIT$', 'WORKORDERID$', 'N'],
                   ['SupportStructure$', 'FEEDERID$', 'TBC'],
                   ['SupportStructure$', 'SHAPE$', ' N'],
                   ['SupportStructure$', 'SUBTYPECD$', 'TBD'],
                   ['SupportStructure$', 'TREATMENTTYPE$', 'N'],
                   ['SupportStructure$', 'TRUENZMG$', 'N'],
                   ['SupportStructure$', 'TYPEOFTOP$', 'N'],
                   ['SupportStructure$', 'USAGETYPE$', 'N']]
    if sap_stat is True:
        for tab_str, att_str, sap_str in bad_doubles:
            data = data[~(data['TABLE'].str.match(tab_str) &
                          data['COLUMN'].str.match(att_str) &
                          data['SAP'].str.match(sap_str))]
        bad_null = [['SurfaceStructure$', 'ENCLOSURE$'],
                    ['SurfaceStructure$', 'ENCLOSUREMANUFACTURER$'],
                    ['SurfaceStructure$', 'ENCLOSURETYPE$'],
                    ['Fuse$', 'ACCURACY$'],
                    ['Fuse$', 'ANCILLARYROLE$'],
                    ['Fuse$', 'ASSETFUNCTION$'],
                    ['Fuse$', 'C_INTJDEID$'],
                    ['Fuse$', 'COMMENTS$'],
                    ['Fuse$', 'CREATIONUSER$'],
                    ['Fuse$', 'DATECREATED$'],
                    ['Fuse$', 'DATEMODIFIED$'],
                    ['Fuse$', 'DEVICETYPE$'],
                    ['Fuse$', 'ELECTRICTRACEWEIGHT$'],
                    ['Fuse$', 'ENABLED$'],
                    ['Fuse$', 'FACILITYID$'],
                    ['Fuse$', 'FEEDERID$'],
                    ['Fuse$', 'FEEDERID2$'],
                    ['Fuse$', 'FEEDERINFO$'],
                    ['Fuse$', 'GEONETFEEDERCODE$'],
                    ['Fuse$', 'GEONETFEEDERID$'],
                    ['Fuse$', 'GEONETSUBSTATION$'],
                    ['Fuse$', 'GLOBALID$'],
                    ['Fuse$', 'INSTALLEDBY$'],
                    ['Fuse$', 'LABELTEXT$'],
                    ['Fuse$', 'LASTUSER$'],
                    ['Fuse$', 'MANUFACTURER$'],
                    ['Fuse$', 'MAXCONTINUOUSCURRENT$'],
                    ['Fuse$', 'MAXINTERRUPTINGCURRENT$'],
                    ['Fuse$', 'MAXOPERATINGVOLTAGE$'],
                    ['Fuse$', 'MOUNTING$'],
                    ['Fuse$', 'NOMINALVOLTAGE$'],
                    ['Fuse$', 'NORMALPOSITION$'],
                    ['Fuse$', 'NUMPHASES$'],
                    ['Fuse$', 'OBJECTID$'],
                    ['Fuse$', 'OPERATINGVOLTAGE$'],
                    ['Fuse$', 'OUTOFORDERINDICATOR$'],
                    ['Fuse$', 'OWNER$'],
                    ['Fuse$', 'PARENTID$'],
                    ['Fuse$', 'PHASEDESIGNATION$'],
                    ['Fuse$', 'PREMISE$'],
                    ['Fuse$', 'PRESENTPOSITION$'],
                    ['Fuse$', 'RDB_UFID$'],
                    ['Fuse$', 'REFERENCE$'],
                    ['Fuse$', 'REMOTECONTROLLED$'],
                    ['Fuse$', 'REMOTEINDICATION$'],
                    ['Fuse$', 'RETICULATION$'],
                    ['Fuse$', 'SCADACONTROLMECHANISM$'],
                    ['Fuse$', 'SCADACONTROLTYPE$'],
                    ['Fuse$', 'SCADAPTID$'],
                    ['Fuse$', 'SHAPE$'],
                    ['Fuse$', 'SITEID$'],
                    ['Fuse$', 'STREETNAME$'],
                    ['Fuse$', 'STREETNO$'],
                    ['Fuse$', 'SUBTYPECD$'],
                    ['Fuse$', 'SUBURB$'],
                    ['Fuse$', 'SYMBOLROTATION$'],
                    ['Fuse$', 'TIMESTAMP$'],
                    ['Fuse$', 'TOWN$'],
                    ['Fuse$', 'TYPE$'],
                    ['Fuse$', 'WORKORDERID$'],
                    ['Fuse$', 'ZONE$']]
        for tab_str, att_str in bad_null:
            data = data[~(data['TABLE'].str.match(tab_str) &
                          data['COLUMN'].str.match(att_str) &
                          data['SAP'].isnull())]
    return data


def up_merge(data1, data2):
    """Panda merge using uppercase."""
    data1['TABLE_up'] = data1['TABLE'].str.upper()
    data1['NAME_up'] = data1['COLUMN'].str.upper()
    data2['TABLE_up'] = data2['TABLE'].str.upper()
    data2['NAME_up'] = data2['COLUMN'].str.upper()
    strp_columns = list(data2)
    strp_columns.remove('TABLE')
    strp_columns.remove('COLUMN')
    data2 = data2[strp_columns]
    fin_data = pd.merge(data1, data2, how='left',
                        left_on=['TABLE_up', 'NAME_up'],
                        right_on=['TABLE_up', 'NAME_up'])
    fin_list = list(fin_data)
    fin_list.remove('TABLE_up')
    fin_list.remove('NAME_up')
    fin_data = fin_data[fin_list]
    return fin_data


def excel_print(data1, data2, data3, data4, data5, data6):
    """This prints out the dataframe in the correct format."""

    list_data = [data1, data2, data3, data4, data5, data6]
    name_list = ['Old elec', 'New elec', 'Old elec dup', 'New elec dup',
                 'Diff After Strip', 'New Elec Before Strip']
    zipped = zip(list_data, name_list)
    excel_writer = pd.ExcelWriter('elec_delta2.xlsx', engine='xlsxwriter')
    for data, name in zipped:
        data.to_excel(excel_writer, sheet_name=name,
                      index=False, freeze_panes=(1, 0))
        num_cols = len(list(data))
        worksheet = excel_writer.sheets[name]
        worksheet.autofilter(0, 0, 0, num_cols-1)
        worksheet.set_column(0, 0, 23.56)
        worksheet.set_column(1, 1, 34.89)
    excel_writer.save()


def script_runner():
    """Runs this script."""

    strp_elec1, strp_elec2 = file_loading()

    strp_elec1 = strp_elec1.drop_duplicates(subset=['TABLE', 'COLUMN', 'SAP'])
    strp_elec1 = old_strip_sql(strp_elec1)
    strp_elec1_dup = strp_elec1[strp_elec1.duplicated(
        subset=['TABLE', 'COLUMN'], keep=False)]
    elec2 = strp_elec2[strp_elec2.duplicated(
        subset=['TABLE', 'COLUMN'], keep=False)]
    strp_elec2 = strp_elec2.drop_duplicates(subset=['TABLE', 'COLUMN', 'SAP'])
    strp_elec2 = strip_sql(strp_elec2)
    strp_elec2_dup = strp_elec2[strp_elec2.duplicated(
        subset=['TABLE', 'COLUMN'], keep=False)]

    fin_dif = pd.concat([strp_elec1, strp_elec2]).drop_duplicates(
        subset=['TABLE', 'COLUMN'], keep=False)

    excel_print(strp_elec1, strp_elec2,
                strp_elec1_dup, strp_elec2_dup, fin_dif, elec2)


script_runner()
