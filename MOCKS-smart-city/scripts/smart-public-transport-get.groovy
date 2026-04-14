def req = mockRequest.getRequest()
def q = { key ->
    def vals = req?.getParameterValues(key)
    return (vals != null && vals.length > 0) ? String.valueOf(vals[0]) : null
}

def status = q("status")
if (status != null) status = status.toLowerCase()
if (status == "in-service")    return "in-service"
if (status == "delayed")       return "delayed"
if (status == "suspended")     return "suspended"
if (status == "out-of-service") return "out-of-service"
if (status == "maintenance")   return "maintenance"
if (status == "end-of-line")   return "end-of-line"

def vehicleType = q("vehicleType")
if (vehicleType != null) vehicleType = vehicleType.toLowerCase()
if (vehicleType == "bus")   return "bus"
if (vehicleType == "tram")  return "tram"
if (vehicleType == "metro") return "metro"
if (vehicleType == "ferry") return "ferry"

def zoneId = q("zoneId")
if (zoneId != null) zoneId = zoneId.toUpperCase()
if (zoneId == "Z-CENTRO") return "by_zone_centro"
if (zoneId == "Z-NORD")   return "by_zone_nord"
if (zoneId == "Z-SUD")    return "by_zone_sud"
if (zoneId == "Z-EST")    return "by_zone_est"

return "example_list"
