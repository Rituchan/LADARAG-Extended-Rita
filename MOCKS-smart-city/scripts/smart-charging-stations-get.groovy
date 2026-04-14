def req = mockRequest.getRequest()
def q = { key ->
    def vals = req?.getParameterValues(key)
    return (vals != null && vals.length > 0) ? String.valueOf(vals[0]) : null
}

def available = q("available")
if (available == "true")  return "available_true"
if (available == "false") return "available_false"

def chargingSpeed = q("chargingSpeed")
if (chargingSpeed != null) chargingSpeed = chargingSpeed.toLowerCase()
if (chargingSpeed == "slow")        return "slow"
if (chargingSpeed == "fast")        return "fast"
if (chargingSpeed == "rapid")       return "rapid"
if (chargingSpeed == "ultra_rapid") return "ultra_rapid"

def status = q("status")
if (status != null) status = status.toLowerCase()
if (status == "operational") return "operational"
if (status == "busy")        return "busy"
if (status == "maintenance") return "maintenance"
if (status == "offline")     return "offline"

def zoneId = q("zoneId")
if (zoneId != null) zoneId = zoneId.toUpperCase()
if (zoneId == "Z-CENTRO") return "by_zone_centro"
if (zoneId == "Z-NORD")   return "by_zone_nord"
if (zoneId == "Z-SUD")    return "by_zone_sud"
if (zoneId == "Z-EST")    return "by_zone_est"

return "example_list"
