def req = mockRequest.getRequest()
def q = { key ->
    def vals = req?.getParameterValues(key)
    return (vals != null && vals.length > 0) ? String.valueOf(vals[0]) : null
}

def status = q("status")
if (status != null) status = status.toLowerCase()
if (status == "normal")      return "normal"
if (status == "full")        return "full"
if (status == "overflowing") return "overflowing"
if (status == "maintenance") return "maintenance"
if (status == "offline")     return "offline"

def binType = q("binType")
if (binType != null) binType = binType.toLowerCase()
if (binType == "general")    return "general"
if (binType == "recyclable") return "recyclable"
if (binType == "organic")    return "organic"
if (binType == "glass")      return "glass"
if (binType == "electronic") return "electronic"

def zoneId = q("zoneId")
if (zoneId != null) zoneId = zoneId.toUpperCase()
if (zoneId == "Z-CENTRO") return "by_zone_centro"
if (zoneId == "Z-NORD")   return "by_zone_nord"
if (zoneId == "Z-SUD")    return "by_zone_sud"
if (zoneId == "Z-EST")    return "by_zone_est"

return "example_list"
