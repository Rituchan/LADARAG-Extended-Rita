def req = mockRequest.getRequest()
def q = { key ->
    def vals = req?.getParameterValues(key)
    return (vals != null && vals.length > 0) ? String.valueOf(vals[0]) : null
}

def status = q("status")
if (status != null) status = status.toLowerCase()
if (status == "operational") return "operational"
if (status == "evacuated")   return "evacuated"
if (status == "maintenance") return "maintenance"
if (status == "closed")      return "closed"

def buildingType = q("buildingType")
if (buildingType != null) buildingType = buildingType.toLowerCase()
if (buildingType == "healthcare")   return "healthcare"
if (buildingType == "education")    return "education"
if (buildingType == "office")       return "office"
if (buildingType == "residential")  return "residential"
if (buildingType == "commercial")   return "commercial"
if (buildingType == "public")       return "public"
if (buildingType == "industrial")   return "industrial"

def zoneId = q("zoneId")
if (zoneId != null) zoneId = zoneId.toUpperCase()
if (zoneId == "Z-CENTRO") return "by_zone_centro"
if (zoneId == "Z-NORD")   return "by_zone_nord"
if (zoneId == "Z-SUD")    return "by_zone_sud"
if (zoneId == "Z-EST")    return "by_zone_est"

return "example_list"
