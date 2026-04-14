def req = mockRequest.getRequest()
def q = { key ->
    def vals = req?.getParameterValues(key)
    return (vals != null && vals.length > 0) ? String.valueOf(vals[0]) : null
}

def available = q("available")
if (available == "true")  return "available_true"
if (available == "false") return "available_false"

def spotType = q("spotType")
if (spotType != null) spotType = spotType.toLowerCase()
if (spotType == "standard")   return "standard"
if (spotType == "disabled")   return "disabled"
if (spotType == "electric")   return "electric"
if (spotType == "motorcycle") return "motorcycle"
if (spotType == "reserved")   return "reserved"

def status = q("status")
if (status != null) status = status.toLowerCase()
if (status == "free")        return "free"
if (status == "occupied")    return "occupied"
if (status == "reserved")    return "reserved"
if (status == "maintenance") return "maintenance"

def zoneId = q("zoneId")
if (zoneId != null) zoneId = zoneId.toUpperCase()
if (zoneId == "Z-CENTRO") return "by_zone_centro"
if (zoneId == "Z-NORD")   return "by_zone_nord"
if (zoneId == "Z-SUD")    return "by_zone_sud"
if (zoneId == "Z-EST")    return "by_zone_est"

return "example_list"
