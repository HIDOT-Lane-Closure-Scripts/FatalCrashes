#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import arcgis
from arcgis.gis import GIS
import smtplib, ssl
import logging, re, os
import arcpy, shutil, sys   
import xml.dom.minidom as DOM
from arcpy import env
import unicodedata
import datetime, tzlocal
from datetime import date , timedelta, datetime
from time import sleep
import math,random
from os import listdir
from arcgis.features._data.geodataset.geodataframe import SpatialDataFrame
from cmath import isnan
from math import trunc
from _server_admin.geometry import Point
from pandas.core.computation.ops import isnumeric
try:
    import urllib.request, urllib.error, urllib.parse  # Python 2
except ImportError:
    import urllib.request as urllib2  # Python 3
import zipfile
from zipfile import ZipFile
import json
import fileinput
from os.path import isdir, isfile, join
import pandas as pd
#from pandas import DataFrame as pdf
#import geopandas as gpd
from arcgis import geometry
from arcgis import features 
import arcgis.network as network
from arcgis.features.analyze_patterns import interpolate_points
import arcgis.geocoding as geocode
from arcgis.features.find_locations import trace_downstream
from arcgis.features.use_proximity import create_buffers
from arcgis.features import GeoAccessor as gac, GeoSeriesAccessor as gsac
from arcgis.features import SpatialDataFrame as spedf
from arcgis.features import FeatureLayer
import numpy as np
from copy import deepcopy
try:
    # ArcGIS user credentials to authenticate against the portal
    credentials = { 'userName' : '****', 'passWord' : '*******'} # credentials = { 'userName' : 'dot_cdelatorre', 'passWord' : 'Bongo123'}
    userName = credentials['userName'] # arcpy.GetParameter(2) # 
    passWord =  credentials['passWord'] #  # arcpy.GetParameter(3) # "ChrisMaz"
    #credentials = { 'userName' : arcpy.GetParameter(4), 'passWord' : arcpy.GetParameter(5)}
    # Address of your ArcGIS portal
    portal_url = r"http://histategis.maps.arcgis.com/"
    print("Connecting to {}".format(portal_url))
    #logger.info("Connecting to {}".format(portal_url))
    #qgis = GIS(portal_url, userName, passWord)
    #qgis = GIS(profile="hisagolprof")
    numfs = 1000 # number of items to query
    #print(f"Connected to {qgis.properties.portalHostname} as {qgis.users.me.username}")
    #logger.info(f"Connected to {qgis.properties.portalHostname} as {qgis.users.me.username}")

    import arcpy, shutil, sys
    import xml.dom.minidom as DOM
    from arcpy import env
    import unicodedata
    import datetime, tzlocal
    from datetime import date , timedelta, datetime
    from time import sleep
    import math,random
    from os import listdir
    from arcgis.features._data.geodataset.geodataframe import SpatialDataFrame
    from cmath import isnan
    from math import trunc
    from _server_admin.geometry import Point
    from pandas.core.computation.ops import isnumeric
    # email to hidotlaneclsoures@hawaii.gov through outlook # Python 3

except ImportError:
#import ago
    print("error {} ".format(ImportError) )
    # 
    SystemExit()


# In[ ]:


def webexsearch(mgis, title, owner_value, item_type_value, max_items_value=1000,inoutside=False):
    item_match = None
    search_result = mgis.content.search(query= "title:{} AND owner:{}".format(title,owner_value), 
                                          item_type=item_type_value, max_items=max_items_value, outside_org=inoutside)
    if "Imagery Layer" in item_type_value:
        item_type_value = item_type_value.replace("Imagery Layer", "Image Service")
    elif "Layer" in item_type_value:
        item_type_value = item_type_value.replace("Layer", "Service")
    
    for item in search_result:
        if item.title == title:
            item_match = item
            break
    return item_match

def lyrsearch(lyrlist, lyrname):
    lyr_match = None
   
    for lyr in lyrlist:
        if lyr.properties.name == lyrname:
            lyr_match = lyr
            break
    return lyr_match

def create_section(lyr, hdrow, chdrows,rtefeat):
    try:
        object_id = 1
        pline = geometry.Polyline(rtefeat)
        feature = features.Feature(
            geometry=pline[0],
            attributes={
                'OBJECTID': object_id,
                'PARK_NAME': 'My Park',
                'TRL_NAME': 'Foobar Trail',
                'ELEV_FT': '5000'
            }
        )

        lyr.edit_features(adds=[feature])
        #_map.draw(point)

    except Exception as e:
        print("Couldn't create the feature. {}".format(str(e)))

def fldvartxt(fldnm,fldtyp,fldnull,fldPrc,fldScl,fldleng,fldalnm,fldreq):
    fld = arcpy.Field()
    fld.name = fldnm
    fld.type = fldtyp
    fld.isNullable = fldnull
    fld.precision = fldPrc
    fld.scale = fldScl
    fld.length = fldleng
    fld.aliasName = fldalnm
    fld.required = fldreq
    return fld

def df_colsame(df):
    """ returns an empty data frame with the same column names and dtypes as df """
    #df0 = pd.DataFrame.spatial({i[0]: pd.Series(dtype=i[1]) for i in df.dtypes.iteritems()}, columns=df.dtypes.index)
    return df

def offdirn(closide,dirn1):
    if closide == 'Right':
        offdirn1 = 'RIGHT'
    elif closide == 'Left':
        offdirn1 = 'LEFT'
        dirn1 = -1*dirn1
    elif closide == 'Center':
        offdirn1 = 'RIGHT'
        dirn1 = 0.5
    elif closide == 'Both':
        offdirn1 = 'RIGHT'
        dirn1 = 0
    elif closide == 'Directional':
        if dirn1 == -1:
            offdirn1 = 'LEFT'
        else:
            offdirn1 = 'RIGHT'
    elif closide == 'Full' or closide == 'All':
        offdirn1 = 'RIGHT'
        dirn1 = 0
    elif closide == 'Shift':
        offdirn1 = 'RIGHT'
    elif closide == 'Local':
        offdirn1 = 'RIGHT'
    else:
        offdirn1 = 'RIGHT'
        dirn1 = 0 
    return offdirn1,dirn1

def deleteupdates(prjstlyrsrc, sectfeats):
    for x in prjstlyrsrc:
        print (" layer: {} ; from item : {} ; URL : {} ; Container : {} ".format(x,x.fromitem,x.url,x.container))
        if 'Projects' in (prjstlyrsrc):
            xfeats =  x.query().features
            if len(xfeats) > 0:
                if isinstance(xfeats,(list,tuple)):
                    if "OBJECTID" in xfeats[0].attributes:
                        oids = "'" + "','".join(str(xfs.attributes['OBJECTID']) for xfs in xfeats if 'OBJECTID' in xfs.attributes ) + "'"
                        oidqry = " OBJECTID in ({}) ".format(oids)
                    elif "OID" in xfeats[0].attributes:    
                        oids = "'" + "','".join(str(xfs.attributes['OID']) for xfs in xfeats if 'OID' in xfs.attributes ) + "'"
                        oidqry = " OID in ({}) ".format(oids)
                    print (" from item : {} ; oids : {} ; ".format(x.fromitem,oids))
                    
                elif isinstance(xfeats,spedf):
                    if "OBJECTID" in xfeats.columns:
                        oids = "'" + "','".join(str(f1.get_value('OBJECTID')) for f1 in xfeats ) + "'"
                        oidqry = " OBJECTID in ({}) ".format(oids)
                    elif "OID" in xfeats.columns:    
                        oids = "'" + "','".join(str(f1.get_value('OID')) for f1 in xfeats ) + "'"
                        oidqry = " OID in ({}) ".format(oids)
                    print (" from item : {} ; oids : {} ; ".format(x.fromitem,oids))
                    
                if 'None' in oids:
                    print (" from item : {} ; oids : {} ; ".format(x.fromitem,oids))
                else:
                    x.delete_features(where=oidqry)

# Given anydate and n1 as 0 or 1 or 2 , etc  it computes Last Friday, First Friday and Second Friday, etc at 4PM
def fridaywk(bdate,n1):
    wkdte = datetime.strftime(bdate,"%w") # + datetime.strftime(bdate,"%z")
    date4pm = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(hours=16)
    fr4pm= date4pm + timedelta(days=(5-int(wkdte)+(n1-1)*7))
    return fr4pm

def intextold(intxt,rte,rtename):
    intshortlbl = intxt['address']['ShortLabel']
    intsplitxt = intshortlbl.split(sep='&', maxsplit=1)
    txtret=intsplitxt[1]  # default to the second intersection unless the second one has the route
    for txt in intsplitxt:
        if rtename not in txt or rte not in txt:
            txtret = txt
    return txtret     

def intext(intxt,rte,rtename,fromtxt="Nothing"):
    intshortlbl = intxt['address']['ShortLabel']
    rtext = re.sub("-","",rte)
    if rtename is None:
        rtenametxt = 'Nothing'
    else:    
        rtenametxt = re.sub("-","",rtename)
    intsplitxt = intshortlbl.split(sep='&') #, maxsplit=1)
    intsplitxt = [t1.strip() for t1 in intsplitxt ]
    if len(intsplitxt)==2: 
        txtret=intsplitxt[1]  # default to the second intersection unless the second one has the route
    elif len(intsplitxt)==3:
        txtret=intsplitxt[2]  # default to the second intersection unless the second one has the route
    else:
        txtret=intsplitxt[0]  # default to the second intersection unless the second one has the route
            
    rtenmsplit = [ t2.strip() for t2 in rtenametxt.split(sep=" ")]
    if len(rtenmsplit)>2:
        rtenmsplit = "{} {}".format(rtenmsplit[0].capitalize(),rtenmsplit[1].capitalize())
    else:
        rtenmsplit = "{}".format(rtenmsplit[0].capitalize())
               
    for txt in intsplitxt:
        txtsep = txt.split(sep=" ")
        if len(txtsep)<=2 :
            if (txt[0:2]).isnumeric():
                txt = "Exit " + txt.upper()
        if rtenmsplit not in txt and rtext not in txt and fromtxt!=txt:
            txtret = txt
        else:
            txtret = txt
    return txtret        

def datemidnight(bdate):
    date0am = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(hours=0)
    return date0am

# function to return whether the closure date range is a weekend or weekday
def wkend(b,e):
    if b==0 and e <=1: 
        return 1 
    elif b>=1 and b<=5 and e>=1 and e<=5: 
        return 0 
    elif b>=5 and (e==6 or e==0): 
        return 1 
    else: 
        return 0

def beginwk(bdate):
    wkdte = datetime.strftime(bdate,"%w")
    if (wkdte==0):
        bw = bdate + timedelta(days=wkdte)
    else:  # wkdte>=1:
        bw = bdate + timedelta(days=(7-wkdte))
    return bw

def beginthiswk(bdate):
    wkdte = datetime.strftime(bdate,"%w")
    if (wkdte==0):
        bw = bdate - timedelta(days=wkdte)
    else:  # wkdte>=1:
        bw = bdate - timedelta(days=(8-int(wkdte)))
    return bw                    

def midnextnight(bdate,n1):
    datenextam = datetime.strptime(datetime.strftime(bdate,"%Y-%m-%d"),"%Y-%m-%d") + timedelta(day=n1)
    return datenextam

#BeginDateName,EndDateName:  The month and the day portion of the begin or end date. (ex. November 23)
def dtemon(dte):
    dtext = datetime.strftime(dte-timedelta(hours=10),"%B") + " " +  str(int(datetime.strftime(dte-timedelta(hours=10),"%d")))
    return dtext

# BeginDay, EndDay: Weekday Name of the begin date (Monday, Tuesday, Wednesday, etc.)
def daytext(dte):
    dtext = datetime.strftime(dte-timedelta(hours=10),"%A") 
    return dtext

#BeginTime, EndTime: The time the lane closure begins.  12 hour format with A.M. or P.M. at the end
def hrtext(dte):
    hrtext = datetime.strftime(dte-timedelta(hours=10),"%I:%M %p") 
    return hrtext

def rtempt(lyrts,rtefc,lrte,bmpvalx,offs=0,fldrte='Route'):
    if 'mptbl' in locals():
        if arcpy.Exists(mptbl):    
            if int(arcpy.GetCount_management(mptbl).getOutput(0)) > 0:
                arcpy.DeleteRows_management(mptbl)
    else:
        rtevenTbl = "RtePtEvents"
        eveLinlyr = "lrtelyr" #os.path.join('in_memory','lrtelyr')
        eveLRSFC = "RtePtEvtFC"
        outFeatseed = "EvTbl"
        x1 = 1001 #random.randrange(1001,2000,1)
        lrsGeoPTbl = """LRS_{}{}""".format(outFeatseed,x1) # DynaSeg result feature table created from LRS points location along routes 
        outfeatbl = """Rt{}""".format(outFeatseed) 
        OidFld = fldvartxt("ObjectID","LONG",False,28,"","","OID",True) 
        # create the bmp and direction field for the merged result table 
        RteFld = fldvartxt(fldrte,"TEXT",False,"","",60,fldrte,True) 
        fldrte = RteFld.name
        # create the bmp and direction field for the merged result table 
        bmpFld = fldvartxt("BMP","DOUBLE",False,18,11,"","BMP",True) 

        # create the emp and direction field for the result table 
        #empFld = arcpy.Field()
        empFld = fldvartxt("EMP","DOUBLE",True,18,11,"","EMP",False) 
        ofFld = fldvartxt("Offset","DOUBLE",True,18,11,"","Offset",False) 
        # linear reference link properties 
        eveProPts = "{} POINT BMP ".format(fldrte)
        eveProLines = "{} LINE BMP EMP".format(fldrte)
        mptbl = str(arcpy.CreateTable_management("in_memory","{}{}".format(rtevenTbl,x1)).getOutput(0))
        # add BMP , EMP and RteDirn fields to the linear reference lane closure table
        #arcpy.AddField_management(mptbl, OidFld.name, OidFld.type, OidFld.precision, OidFld.scale)
        arcpy.AddField_management(mptbl, RteFld.name, RteFld.type, RteFld.precision, RteFld.scale)
        arcpy.AddField_management(mptbl, bmpFld.name, bmpFld.type, bmpFld.precision, bmpFld.scale)
        arcpy.AddField_management(mptbl, empFld.name, empFld.type, empFld.precision, empFld.scale)
        arcpy.AddField_management(mptbl, ofFld.name, ofFld.type, ofFld.precision, ofFld.scale)
        # linear reference fields 

    bmpval = bmpvalx
    empval = bmpvalx
    rteFCSel = "RteSelected"
    rtevenTbl = "RteLinEvents"
    eveLinlyr = "lrtelyr" #os.path.join('in_memory','lrtelyr')
    arcpy.env.overwriteOutput = True
    if (len(rtefc)>0):
        flds = ['OBJECTID', 'SHAPE@JSON', 'ROUTE'] # selected fields in Route
        lrterows = arcpy.da.SearchCursor(lrte,flds)
        mpinscur.insertRow((rteid.upper(), bmpval,offs))
        dirnlbl = 'LEFT'
        arcpy.MakeRouteEventLayer_lr(lrte,fldrte,mptbl,eveProPts, eveLinlyr,ofFld.name,"ERROR_FIELD","ANGLE_FIELD",'NORMAL','ANGLE',dirnlbl)
        # get the geoemtry from the result layer and append to the section feature class
        if arcpy.Exists(eveLinlyr):    
            cntLyr = arcpy.GetCount_management(eveLinlyr)
        if cntLyr.outputCount > 0:
            #lrsectfldnms = [ f.name for f in arcpy.ListFields(eveLinlyr)]
            insecgeo = None
            # dynamic segementaiton result layer fields used to create the closure segment  
            lrsectfldnms = ['ObjectID', 'Route', 'BMP', 'Shape@JSON']
            evelincur = arcpy.da.SearchCursor(eveLinlyr,lrsectfldnms)
            for srow in evelincur:
                #insecgeo = srow.getValue("SHAPE@")
                #print("id : {} , Rte : {} , BMP {} , EMP : {} , Geom : {} ".format(srow[0],srow[1],srow[2],srow[3],srow[4]))
                rtenum = srow[1]
                insecgeo = arcgis.geometry.Geometry(srow[4])
                if insecgeo == None:
                    print('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,bmpval,empval,offs ))
                    logger.info('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,bmpval,empval,offs ))
                    insecgeo = geomrte.project_as(sprefwgs84)
                else:
                    print('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,bmpval,empval,offs ))
                    logger.info('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,bmpval,empval,offs ))
                insecgeo = insecgeo.project_as(sprefwgs84)
            del evelincur        
        del rteFCSel,lrte,rtevenTbl  
    else:
        rteidx = "460"  # Molokaii route 0 to 15.55 mileage
        print('Route {} not found using {} create point geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrts,rteid,bmpvalx,bmpval,empval,offs ))
        logger.info('Route {} not found using {} to create point geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrts,rteid,bmpvalx,bmpval,empval,offs ))
        featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteidx,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
        ftlnclrte = featlnclrte.features
        if (len(ftlnclrte)>0):
            rtegeo = ftlnclrte[0].geometry
            geomrte = arcgis.geometry.Geometry(rtegeo)
            insecgeo = geomrte.project_as(sprefwgs84)
        else:
            insecgeo=None    
    return insecgeo

def rtesectpt(lyrts,rteid,bmpvalx,offs,fldrte='Route'):
    if 'mptbl' in locals():
        if arcpy.Exists(mptbl):    
            if int(arcpy.GetCount_management(mptbl).getOutput(0)) > 0:
                arcpy.DeleteRows_management(mptbl)
    else:
        rtevenTbl = "RtePtEvents"
        eveLinlyr = "lrtelyr" #os.path.join('in_memory','lrtelyr')
        eveLRSFC = "RtePtEvtFC"
        outFeatseed = "EvTbl"
        x1 = 1001 #random.randrange(1001,2000,1)
        lrsGeoPTbl = """LRS_{}{}""".format(outFeatseed,x1) # DynaSeg result feature table created from LRS points location along routes 
        outfeatbl = """Rt{}""".format(outFeatseed) 
        OidFld = fldvartxt("ObjectID","LONG",False,28,"","","OID",True) 
        # create the bmp and direction field for the merged result table 
        RteFld = fldvartxt(fldrte,"TEXT",False,"","",60,fldrte,True) 
        fldrte = RteFld.name
        # create the bmp and direction field for the merged result table 
        bmpFld = fldvartxt("BMP","DOUBLE",False,18,11,"","BMP",True) 

        # create the emp and direction field for the result table 
        #empFld = arcpy.Field()
        empFld = fldvartxt("EMP","DOUBLE",True,18,11,"","EMP",False) 
        ofFld = fldvartxt("Offset","DOUBLE",True,18,11,"","Offset",False) 
        # linear reference link properties 
        eveProPts = "{} POINT BMP EMP".format(fldrte)
        eveProLines = "{} LINE BMP EMP".format(fldrte)
        mptbl = str(arcpy.CreateTable_management("in_memory","{}{}".format(rtevenTbl,x1)).getOutput(0))
        # add BMP , EMP and RteDirn fields to the linear reference lane closure table
        #arcpy.AddField_management(mptbl, OidFld.name, OidFld.type, OidFld.precision, OidFld.scale)
        arcpy.AddField_management(mptbl, RteFld.name, RteFld.type, RteFld.precision, RteFld.scale)
        arcpy.AddField_management(mptbl, bmpFld.name, bmpFld.type, bmpFld.precision, bmpFld.scale)
        arcpy.AddField_management(mptbl, empFld.name, empFld.type, empFld.precision, empFld.scale)
        arcpy.AddField_management(mptbl, ofFld.name, ofFld.type, ofFld.precision, ofFld.scale)
        # linear reference fields 
    bmpval = bmpvalx
    empval = bmpvalx
    rteFCSel = "RteSelected"
    rtevenTbl = "RteLinEvents"
    eveLinlyr = "lrtelyr" #os.path.join('in_memory','lrtelyr')
    arcpy.env.overwriteOutput = True
    featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteid,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
    if (len(featlnclrte)<=0):
        if rteid == "5600":
            rteid="560"
        else:
            rteid="560"    
        featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteid,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
    if (len(featlnclrte)>0):
        rteFCSel = featlnclrte.save('in_memory','rtesel')
        ftlnclrte = featlnclrte.features
        rtegeo = ftlnclrte[0].geometry
        geomrte = arcgis.geometry.Geometry(rtegeo,sr=sprefwebaux)
        rtepaths = rtegeo['paths']
        rtept1 = rtepaths[0][0] # geomrte.first_point
        rtept2 = rtepaths[0][len(rtepaths[0])-1] #geomrte.last_point
        bmprte = round(rtept1[2],3)
        emprte = round(rtept2[2],3)
                
        if (bmpval<bmprte):
            bmpval=bmprte
        if (bmpval>emprte):
            bmpval=bmprte
    
        #rteFCSel = featlnclrte.save(lcfgdboutpath,'rtesel')
        arcpy.env.outputMFlag = "Disabled"
        lrte = os.path.join('in_memory','rteselyr')
        arcpy.CreateRoutes_lr(rteFCSel,RteFld.name, lrte, "TWO_FIELDS", bmpFld.name, empFld.name)
        flds = ['OBJECTID', 'SHAPE@JSON', 'ROUTE'] # selected fields in Route
        lrterows = arcpy.da.SearchCursor(lrte,flds)
        # create the milepost insert cursor fields  
        mpflds = [RteFld.name,bmpFld.name,empFld.name,ofFld.name]
        # create the MilePost Insert cursor 
        mpinscur = arcpy.da.InsertCursor(mptbl, mpflds)  
        
        mpinscur.insertRow((rteid.upper(), bmpval,bmpval,offs))
        dirnlbl = 'LEFT'
        arcpy.MakeRouteEventLayer_lr(lrte,fldrte,mptbl,eveProPts, eveLinlyr,ofFld.name,"ERROR_FIELD","ANGLE_FIELD",'NORMAL','ANGLE',dirnlbl)
        # get the geoemtry from the result layer and append to the section feature class
        if arcpy.Exists(eveLinlyr):    
            cntLyr = arcpy.GetCount_management(eveLinlyr)
        if cntLyr.outputCount > 0:
            #lrsectfldnms = [ f.name for f in arcpy.ListFields(eveLinlyr)]
            insecgeo = None
            # dynamic segementaiton result layer fields used to create the closure segment  
            lrsectfldnms = ['ObjectID', 'Route', 'BMP', 'EMP', 'Shape@JSON']
            evelincur = arcpy.da.SearchCursor(eveLinlyr,lrsectfldnms)
            for srow in evelincur:
                #insecgeo = srow.getValue("SHAPE@")
                #print("id : {} , Rte : {} , BMP {} , EMP : {} , Geom : {} ".format(srow[0],srow[1],srow[2],srow[3],srow[4]))
                rtenum = srow[1]
                insecgeo = arcgis.geometry.Geometry(srow[4])
                if insecgeo == None:
                    print('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
                    logger.info('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
                    insecgeo = geomrte.project_as(sprefwgs84).first_point
                else:
                    print('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
                    logger.info('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
                insecgeo = insecgeo.project_as(sprefwgs84)
            del evelincur        
        del rteFCSel,lrte,rtevenTbl  
    else:
        rteidx = "460"  # Molokaii route 0 to 15.55 mileage
        print('Route {} not found using {} create section geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
        logger.info('Route {} not found using {} to create section geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrts,rteid,bmpval,empvalx,bmpval,empval,offs ))
        featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteidx,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
        ftlnclrte = featlnclrte.features
        if (len(ftlnclrte)>0):
            rtegeo = ftlnclrte[0].geometry
            geomrte = arcgis.geometry.Geometry(rtegeo.first_point)
            insecgeo = geomrte.project_as(sprefwgs84)
        else:
            insecgeo=None    
    return insecgeo


def rtesectmp(lyrts,rteid,bmpvalx,empvalx,offs):
    if arcpy.Exists(mptbl):    
        if int(arcpy.GetCount_management(mptbl).getOutput(0)) > 0:
            arcpy.DeleteRows_management(mptbl)
    bmpval = bmpvalx
    empval = empvalx
    rteFCSel = "RteSelected"
    rtevenTbl = "RteLinEvents"
    eveLinlyr = "lrtelyr" #os.path.join('in_memory','lrtelyr')
    arcpy.env.overwriteOutput = True
    featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteid,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
    if (len(featlnclrte)<=0):
        if rteid == "5600":
            rteid="560"
        else:
            rteid="560"    
        featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteid,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
    if (len(featlnclrte)>0):
        rteFCSel = featlnclrte.save('in_memory','rtesel')
        ftlnclrte = featlnclrte.features
        rtegeo = ftlnclrte[0].geometry
        geomrte = arcgis.geometry.Geometry(rtegeo,sr=sprefwebaux)
        rtepaths = rtegeo['paths']
        rtept1 = rtepaths[0][0] # geomrte.first_point
        rtept2 = rtepaths[0][len(rtepaths[0])-1] #geomrte.last_point
        bmprte = round(rtept1[2],3)
        emprte = round(rtept2[2],3)
        if (empval<bmpval):
            inpval = empval
            empval=bmpval
            bmpval = inpval
        elif (round(empval,3)==0 and bmpval<=0):
            empval=bmpval + 0.01
                
        if (bmpval<bmprte):
            bmpval=bmprte
        if (bmpval>emprte):
            bmpval=bmprte
        if (empval>emprte):
            empval=emprte
    
        #rteFCSel = featlnclrte.save(lcfgdboutpath,'rtesel')
        arcpy.env.outputMFlag = "Disabled"
        lrte = os.path.join('in_memory','rteselyr')
        arcpy.CreateRoutes_lr(rteFCSel,RteFld.name, lrte, "TWO_FIELDS", bmpFld.name, empFld.name)
        flds = ['OBJECTID', 'SHAPE@JSON', 'ROUTE'] # selected fields in Route
        lrterows = arcpy.da.SearchCursor(lrte,flds)
        
        if (abs(empval-bmpval)<0.01):
            bmpval=max(bmpval,empval)-0.005
            empval=bmpval+0.01
        mpinscur.insertRow((rteid.upper(), bmpval,empval,offs))
        dirnlbl = 'LEFT'
        arcpy.MakeRouteEventLayer_lr(lrte,fldrte,mptbl,eveProLines, eveLinlyr,ofFld.name,"ERROR_FIELD","ANGLE_FIELD",'NORMAL','ANGLE',dirnlbl)
        # get the geoemtry from the result layer and append to the section feature class
        if arcpy.Exists(eveLinlyr):    
            cntLyr = arcpy.GetCount_management(eveLinlyr)
        if cntLyr.outputCount > 0:
            #lrsectfldnms = [ f.name for f in arcpy.ListFields(eveLinlyr)]
            insecgeo = None
            # dynamic segementaiton result layer fields used to create the closure segment  
            lrsectfldnms = ['ObjectID', 'Route', 'BMP', 'EMP', 'Shape@JSON']
            evelincur = arcpy.da.SearchCursor(eveLinlyr,lrsectfldnms)
##            for srow in evelincur:
##                #insecgeo = srow.getValue("SHAPE@")
##                #print("id : {} , Rte : {} , BMP {} , EMP : {} , Geom : {} ".format(srow[0],srow[1],srow[2],srow[3],srow[4]))
##                rtenum = srow[1]
##                insecgeo = arcgis.geometry.Geometry(srow[4])
##                if insecgeo == None:
##                    print('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
##                    logger.info('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
##                    insecgeo = geomrte.project_as(sprefwgs84).first_point
##                else:
##                    print('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
##                    logger.info('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
##                insecgeo = insecgeo.project_as(sprefwgs84)
            srow = evelincur.next()
            rtenum = srow[1]
            insecgeo = arcgis.geometry.Geometry(srow[4])
            if insecgeo == None:
                print('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
                logger.info('Not able to create section geometry for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
                insecgeo = geomrte.project_as(sprefwgs84).first_point
            else:
                print('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
                logger.info('created project section for layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(lyrts,rteid,bmpvalx,empval,bmpval,empval,offs ))
            insecgeo = insecgeo.project_as(sprefwgs84)
            del evelincur        
        del rteFCSel,lrte,rtevenTbl  
    else:
        rteidx = "460"  # Molokaii route 0 to 15.55 mileage
        print('Route {} not found using {} create section geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrts,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
        logger.info('Route {} not found using {} to create section geometry layer {} , on Route {} ; original BMP : {} ; EMP : {} ; section BMP : {} ; EMP : {} ; offset {}.'.format(rteid,rteidx,lyrts,rteid,bmpvalx,empvalx,bmpval,empval,offs ))
        featlnclrte = lyrts.query(where = "Route in  ({}{}{}) ".format(" '",rteidx,"' "),return_m=True,out_fields='*') #.sdf # + ,out_fields="globalid")
        ftlnclrte = featlnclrte.features
        if (len(ftlnclrte)>0):
            rtegeo = ftlnclrte[0].geometry
            geomrte = arcgis.geometry.Geometry(rtegeo)
            insecgeo = geomrte.project_as(sprefwgs84)
        else:
            insecgeo=None    
    return insecgeo

def mergeometry(geomfeat):
    mgeom = geomfeat.geometry
    if len(geomfeat)>0:
        rtegeo = geomfeat.geometry
        geomrte = arcgis.geometry.Geometry(rtegeo,sr=sprefwebaux)
        rtepaths = rtegeo['paths']
        rtept1 = rtepaths[0][0] # geomrte.first_point
        glen = len(rtepaths) # rtepaths[0][len(rtepaths[0])-1] 
        rtept2 = rtepaths[glen-1][len(rtepaths[glen-1])-1] #geomrte.last_point
        mgeom =[ [ x for sublist in rtepaths for x in sublist] ]
    return mgeom

def assyncadds(lyr1,fset):
    sucs1=0
    pres = None
    t1 = 0
    while(sucs1<=0 and t1<10):
        pres = lyr1.edit_features(adds=fset) #.append(prjfset) #.append(prjfset,field_mappings=fldmaprj)
        if pres['addResults'][0]['success']==True:
            sucs1=1
        else:
            t1 += 1
            sleep(7)
    return pres

def assyncaddspt(lyr1,fset):
    sucs1=0
    pres = None
    t1 = 0
    while(sucs1<=0 and t1<5):
        pres = lyr1.edit_features(adds=fset) #.append(prjfset) #.append(prjfset,field_mappings=fldmaprj)
        if pres['addResults'][0]['success']==True:
            sucs1=1
        else:
            t1 += 1
            sleep(5)
    return pres

def assyncedits(lyr1,fset):
    sucs1=0
    errcnt = 0
    pres = None
    while(round(sucs1)==0):
        pres = lyr1.edit_features(updates=fset) #.append(prjfset) #.append(prjfset,field_mappings=fldmaprj)
        if pres['updateResults'][0]['success']==True:
            sucs1=1
        else:
            errcnt+=1
            terr = datetime.today().strftime("%A, %B %d, %Y at %H:%M:%S %p")
            print("Attempt {} at {} ; Result : {} ; Layer {} ; attributes {} ; geometry {} ".format(errcnt,terr, pres,lyr1.properties.name,fset.features[0].attributes,fset.features[0].geometry))
            logger.error("Attempt {} at {} ; Result : {} ; Layer {} ; attributes {} ; geometry {} ".format(errcnt,terr, pres,lyr1.properties.name,fset.features[0].attributes,fset.features[0].geometry))
            if errcnt<=10:
                sleep(7)
            else:    
                sucs1 = -1
                print("Attempt {} at {} ; Result : {} ; Layer {} ; attributes {} ; geometry {} \n failed {}  ".format(errcnt,terr, pres,lyr1.properties.name,fset.features[0].attributes,fset.features[0].geometry,sucs1))
                logger.error("Attempt {} at {} ; Result : {} ; Layer {} ; attributes {} ; geometry {} \n failed {} ".format(errcnt,terr, pres,lyr1.properties.name,fset.features[0].attributes,fset.features[0].geometry,sucs1))
    return pres

def qryhistdate(bdate,d1=0):
    dateqry = datetime.strftime((bdate-timedelta(days=d1)),"%m-%d-%Y")
    return dateqry

