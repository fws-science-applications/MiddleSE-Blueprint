#!/usr/bin/env python
# coding: utf-8

# ## Mainstem Big River Condition Index [MBRCI]

# ## Notebook Overview
# This notebook provides the processes needed to prepare the data inputs of the MBRCI. Following preprocessing, the MBRCI Decision Tree is provided.

# ## Libraries and Data Inputs

# In[1]:


#import needed libraries
import arcpy #arcgis library
import pandas as pd 
import numpy as np


# In[2]:


#use these functions developed in a previous model to convert to and from a pandas dataframe/ESRI Table

#the following is a function written to convert ESRI file formats (feature classes, shapefiles, tables) to Pandas dataframes
def table_to_data_frame(in_table, input_fields=None, where_clause=None):
    """Function will convert an arcgis table into a pandas dataframe with an object ID index, and the selected
    input fields using an arcpy.da.SearchCursor."""
    OIDFieldName = arcpy.Describe(in_table).OIDFieldName
    if input_fields:
        final_fields = [OIDFieldName] + input_fields
    else:
        final_fields = [field.name for field in arcpy.ListFields(in_table)]
    data = [row for row in arcpy.da.SearchCursor(in_table, final_fields, where_clause=where_clause)]
    fc_dataframe = pd.DataFrame(data, columns=final_fields)
    fc_dataframe = fc_dataframe.set_index(OIDFieldName, drop=True)
    return fc_dataframe

#write function to convert from dataframe to an ESRI Table
def PandasDF_to_ArcTable(in_df,output_table):
    df_array = np.array(np.rec.fromrecords(in_df.values))
    names = in_df.dtypes.index.tolist()
    df_array.dtype.names = tuple(names)
    arcpy.da.NumPyArrayToTable(df_array,output_table)
    print ("Table exported to defined output filepath (output_table)")


# In[3]:


##Split node 1: Data Inputs [Summarized NHDPlus v2 by HUC12 SECAS Geography] 
#Data pre-summarized to include averages for QA_MA & QE_MA
huc_input = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\BGR_Intersected_HUC12"
huc_fields = ['HUC_12', 'ACRES', 'MEAN_QA_MA','MEAN_QE_MA']
SECAS_HUC12_MBR_Int = table_to_data_frame(huc_input,huc_fields)


# In[4]:


##split node 2: National Inventory of Dams Count per HUC12
NID_input = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\SECAS_HUC12_NID"
fields_in = ['HUC_12','Point_Count']

NID = table_to_data_frame(NID_input,fields_in)

NID = NID.rename(columns = {'Point_Count' : 'NID_Dam_Count'})


# In[5]:


##Split node 3: Data Inputs[IF,Sandbars,Deep Water Refugia]; Initially extent will be set to MiddleSE
#Zonal Statistics to Table ran in preprocessing to summarize sandbar and deepwater refugia data to the HUC12's
fields = ['HUC_12','COUNT']
sandbar_in = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\zonal_sandbar_sn2"
SEIF_in = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\ZonalSt_SEIF_10_100_sn2"
refugia_in = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\refugia_Zonal_sn2"
secondary_channels_in = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\secondary_channels"
sec_chan_fields = ['HUC_12','Point_Count']

#use conversion function to convert from ESRI Table format to a Pandas DF
sandbars = table_to_data_frame(sandbar_in,fields)
refugia = table_to_data_frame(refugia_in,fields)
SEIF = table_to_data_frame(SEIF_in,fields)
secondary_channels = table_to_data_frame(secondary_channels_in,sec_chan_fields)


#rename new df columns names to reflect respective df name
sandbars = sandbars.rename(columns={'COUNT': 'Sandbar_CellCount'})
refugia = refugia.rename(columns={'COUNT': 'refugia_CellCount'})
SEIF = SEIF.rename(columns={'COUNT': 'SEIF_CellCount'})
secondary_channels = secondary_channels.rename(columns = {'Point_Count' : 'Secondary_Channel_Count'})


# In[6]:


##split node 4 data: Sinuosity [Sinuosity calculated in preprocessing]
sinuosity_in = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\SECAS_MBR_Sinuosity_HUC12_sn3"
sinuosity_fields = ['HUC_12','MEAN_sinuosity']

#use conversion function to convert from ESRI Table format to a Pandas DF
sinuosity = table_to_data_frame(sinuosity_in,sinuosity_fields)


# In[7]:


##split node 5 data: Variety of Lateral Connectedness and Proportion of int IF Area in Open Lands
int_connect = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\ZonalS_Connectedness_Intermittent_IF_10_90v2"
perm_connect = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\ZonalS_Connectedness_Perm_IF_90v2"
int_IF_openland = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\Inter_IF_Openland_ZonalHUC12_sn4_1"
Openland_NLCD = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\Openland_NLCD_HUC12"

#fields that we need want to bring into our pandas df
connect_fields = ['HUC_12','VARIETY']
openland_fields = ['HUC_12','COUNT']

#use conversion function to convert from ESRI Table format to a Pandas DF
int_connected = table_to_data_frame(int_connect,connect_fields)
perm_connected = table_to_data_frame(perm_connect,connect_fields)
IF_OpenLands = table_to_data_frame(int_IF_openland,openland_fields)
Openland_NLCD = table_to_data_frame(Openland_NLCD,openland_fields)

#rename cell count and variety fields to be reflective of their respective dataframe
int_connected = int_connected.rename(columns = {'VARIETY':'Intermittent_Variety'})
perm_connected = perm_connected.rename(columns = {'VARIETY':'Perm_Variety'})
IF_Openlands = IF_OpenLands.rename(columns = {'COUNT':'IF_Openlands_Count'})
Openlands = Openland_NLCD.rename(columns = {'COUNT':'Openlands_Count'})


# In[8]:


##need to convert cell counts to area acres [IF_Openlands,SEIF,refugia,sandbars, Openlands]; all are in Albers (meters)
#cellsize list corresponds in order to variables mentioned above
cellsizes = [34.855602, 34.855602, 33.578955, 30, 30]
cellsizes_sqm = [cellsizes ** 2 for cellsizes in cellsizes]
con = [4046.8564224]

IF_Openlands['Openland_acres'] = (IF_Openlands['IF_Openlands_Count'] * cellsizes_sqm[0])/con
SEIF['SEIF_acres'] = (SEIF['SEIF_CellCount'] * cellsizes_sqm[1])/con
refugia['refugia_acres'] = (refugia['refugia_CellCount'] * cellsizes_sqm[2])/con
sandbars['sandbars_acres'] = (sandbars['Sandbar_CellCount'] * cellsizes_sqm[3])/con
Openlands['openland_nlcd_acres'] = (Openlands['Openlands_Count'] * cellsizes_sqm[4])/con

#sandbars needs conversion to hectares
sandbars['sandbar_hectares'] = sandbars['sandbars_acres']/2.471


# ## Creating our foundational dataframe for the MBRCI Decision Tree

# In[9]:


##join Split node data into a single df using a left outer join on 'HUC_12'
MBRCI = SECAS_HUC12_MBR_Int.merge(sandbars, on = fields[0],how='left').merge(refugia, on = fields[0],how='left').merge(SEIF, on = fields[0],how='left').merge(sinuosity, on =fields[0],how='left').merge(int_connected, on = fields[0],how='left').merge(perm_connected, on = fields[0],how='left').merge(IF_Openlands, on = fields[0],how='left').merge(Openlands, on = fields[0], how = 'left').merge(NID, on = fields[0], how = 'left').merge(secondary_channels, on = fields[0], how = 'left')

#calculate openland proportions, by dividing the acreage of IF Openland by the total NLCD Openland
MBRCI['IF_Open_Land_Perc'] = MBRCI['Openland_acres'] / MBRCI['openland_nlcd_acres']

#calculate SN2 percentages
MBRCI['refugia_perc'] = MBRCI['refugia_acres'] / MBRCI['ACRES']
MBRCI['SEIF_perc'] = MBRCI['SEIF_acres'] / MBRCI['ACRES']


# In[11]:


#build conditions for each split node
values = [1,0]


sn2 = [
    (MBRCI['sandbar_hectares'] >= 50) & (MBRCI['refugia_perc'] >= 0.02) & (MBRCI['SEIF_perc'] >= .2) & (MBRCI['Secondary_Channel_Count'] >= 2),
    (MBRCI['sandbar_hectares'] < 50) | (MBRCI['refugia_perc'] < 0.02) | (MBRCI['SEIF_perc'] < .2) | (MBRCI['Secondary_Channel_Count'] < 2)
]

sn3 = [
    (MBRCI['NID_Dam_Count'] == 0),
    (MBRCI['NID_Dam_Count'] > 0)
]

sn4 = [
    (MBRCI['MEAN_sinuosity'] >= 1.2),
    (MBRCI['MEAN_sinuosity'] < 1.2)
]

MBRCI['SN2'] = np.select(sn2,values)
MBRCI['SN3'] = np.select(sn3,values)
MBRCI['SN4'] = np.select(sn4,values)


# In[24]:


#SN5 is actually a multi split, based on how many conditions have been met
sn5_vals = [3,2,2,2,1,1,1,0]

sn5 = [
    (MBRCI['Intermittent_Variety'] >= 6) & (MBRCI['Perm_Variety'] >= 4) & (MBRCI['IF_Open_Land_Perc'] >= .07),
    (MBRCI['Intermittent_Variety'] >= 6) & (MBRCI['Perm_Variety'] >= 4) & (MBRCI['IF_Open_Land_Perc'] < .07),
    (MBRCI['Intermittent_Variety'] < 6) & (MBRCI['Perm_Variety'] >= 4) & (MBRCI['IF_Open_Land_Perc'] >= .07),
    (MBRCI['Intermittent_Variety'] >= 6) & (MBRCI['Perm_Variety'] < 4) & (MBRCI['IF_Open_Land_Perc'] >= .07),
    (MBRCI['Intermittent_Variety'] >= 6) & (MBRCI['Perm_Variety'] < 4) & (MBRCI['IF_Open_Land_Perc'] < .07),
    (MBRCI['Intermittent_Variety'] < 6) & (MBRCI['Perm_Variety'] >= 4) & (MBRCI['IF_Open_Land_Perc'] < .07),
    (MBRCI['Intermittent_Variety'] < 6) & (MBRCI['Perm_Variety'] < 4) & (MBRCI['IF_Open_Land_Perc'] >= .07),
    (MBRCI['Intermittent_Variety'] < 6) & (MBRCI['Perm_Variety'] < 4) & (MBRCI['IF_Open_Land_Perc'] < .07)
]

MBRCI['SN5'] = np.select(sn5,sn5_vals)


# In[25]:


#write function that will produce a list of values from 1-32
def getnums(s, e,i):
   return list(range(s, e,i))

# Driver Code
start, end, intval = 1,33,1
index_values = list(getnums(start, end,intval))


# ## The MBRCI Decision Tree

# In[26]:


#build the conditional tree
index_vals = index_values

indx = [
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 0),#1 0000
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 1),#2 0001
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 2),#3 0002
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 3),#4 0003
    
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 0),#5 0010
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 1),#6 0011
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 2),#7 0012
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 3),#8 0013
    
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 0),#9 0100
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 1),#10 0101
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 2),#11 0102
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 3),#12 0103
    
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 0),#13 0110
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 1),#14 0111
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 2),#15 0112
    (MBRCI['SN2'] == 0) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 3),#16 0113
    
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 0),#17 1000
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 1),#18 1001
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 2),#19 1002
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 3),#20 1003
    
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 0),#21 1010
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 1),#22 1011
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 2),#23 1012
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 0) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 3),#24 1013
        
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 0),#25 1100
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 1),#26 1101
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 2),#27 1102
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 0) & (MBRCI['SN5'] == 3),#28 1103
        
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 0),#29 1110
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 1),#30 1111
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 2),#31 1112
    (MBRCI['SN2'] == 1) & (MBRCI['SN3'] == 1) & (MBRCI['SN4'] == 1) & (MBRCI['SN5'] == 3) #32 1113
]

MBRCI['MBRCI'] = np.select(indx,index_vals)


# In[43]:


#plot distribution of MBRCI classes amongst HUC12's
histdf = MBRCI.groupby('MBRCI').count()
histdf['HUC_12'].plot.bar()


# ## From Dataframe to ESRI

# In[28]:


#set output
MBRCI_out = r"E:\PRAAT\Aquatics\Habitat\Mainstem Big River Condition Index\MBRCI_data_inputs.gdb\MBRCIv2"

PandasDF_to_ArcTable(MBRCI,MBRCI_out)

