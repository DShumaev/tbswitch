import requests
import json
import time
import logging
import os
from logging.handlers import RotatingFileHandler
from pythonping import ping
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from pysnmp import hlapi


# __________________________________________INITIALIZATION BLOCK__________________________________________
ups_priority = "Unknown"

ups_ip = {
    '320.2': "xxx.xxx.xxx.xxx",
    '320.3': "xxx.xxx.xxx.xxx",
    '420.2': "xxx.xxx.xxx.xxx",
    '420.3': "xxx.xxx.xxx.xxx"
}

oid_list = {
    'upsInputVoltage1': "1.3.6.1.2.1.33.1.3.3.1.3.1",
    'upsInputVoltage2': "1.3.6.1.2.1.33.1.3.3.1.3.2",
    'upsInputVoltage3': "1.3.6.1.2.1.33.1.3.3.1.3.3",
    'upsOutputVoltage': "1.3.6.1.2.1.33.1.4.4.1.2.1",
    'upsOutputCurrent': "1.3.6.1.2.1.33.1.4.4.1.3.1",
    'upsOutputPower': "1.3.6.1.2.1.33.1.4.4.1.4.1",
    'upsSecondsOnBattery': "1.3.6.1.2.1.33.1.2.2.0",
    'upsEstimatedMinutesRemaining': "1.3.6.1.2.1.33.1.2.3.0",
    'upsEstimatedChargeRemaining': "1.3.6.1.2.1.33.1.2.4.0",
    'upsBatteryCurrent': "1.3.6.1.2.1.33.1.2.6.0",
    'upsBypassCurrent': "1.3.6.1.2.1.33.1.5.3.1.3.1",
    'upsBypassPower': "1.3.6.1.2.1.33.1.5.3.1.4.1",
    'upsBatteryStatus': "1.3.6.1.2.1.33.1.2.1.0",  # (1)-Unknown (2)-batteryNormal (3)-batteryLow (4)-batteryDepleted
    'upsmgBatteryFaultBattery': "1.3.6.1.4.1.705.1.5.9.0",  # (1)-Yes (2)-No
    'upsmgBatteryChargerFault': "1.3.6.1.4.1.705.1.5.15.0"  # (1)-Yes (2)-No
}

primary_ip = "xxx.xxx.xxx.xxx"       # IP of Primary Data Collector (same IP of VM DSM18)
remote_ip = "xxx.xxx.xxx.xxx"        # IP of Remote Data Collector (same IP of VM DSM18_2)
SHD1_virtual_ip = "xxx.xxx.xxx.xxx"    # IP of virtual management port for SHD1 (SC4020)
SHD2_virtual_ip = "xxx.xxx.xxx.xxx"   # IP of virtual management port for SHD2 (SC4020)
port = "3033"
current_ip_dc = "unknown"       # have primary_ip or remote_ip value
primary_hostname = "name.your_domain"
remote_hostname = "name.your_domain"
user_name = "your_user"
password = "your_password"
api_version = "4.1"         # version API, which used client, must be not upper of version REST API Data Collector
verify_cert = False
tb_active_flag = False      # flag of the status of tiebreaker
previous_response_successful = False
SHD1_state_up = False       # flag of the status of SHD1 (SC4020)
SHD2_state_up = False       # flag of the status of SHD2 (SC4020)
SHD1_state_up_previous = False
SHD2_state_up_previous = False
dc_primary_state_up = False
dc_remote_state_up = False
dc_primary_state_up_previous = False
dc_remote_state_up_previous = False
dc_reboot_detected = False
SHD1_port_rebalance_needed = False
SHD2_port_rebalance_needed = False
attempt_count = 0           # for non optimal state of system
time_delay = 10             # time delay fot main cycle
local_tiebreaker_use = "Default"    # flag show which local tiebreaker.
                                    # flag can have 3 values: "Default", "Primary", "Remote"
dc_and_tb_state = {    # data collector and tiebreaker states
    primary_ip: "Primary",
    remote_ip: "Remote",
    "unknown_ip": "Default"
}

name_volume = {
    'first': "HistorianVolume1",    # name of volume on SHD1
    'second': "HistorianVolume2"    # name of volume on SHD2
}

primary_volume_location = "Unknown"  # HistorianVolume1 or HistorianVolume2
if not verify_cert:
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

header = {'Content-Type': 'application/json; charset=utf-8',
          'Accept': 'application/json',
          'x-dell-api-version': api_version}

url_dict = {
    'login': "ApiConnection/Login",
    'logout': "ApiConnection/Logout",
    'api_connect': "ApiConnection/ApiConnection",
    'SC_array_list': "ApiConnection/ApiConnection/{instance_ID}/StorageCenterList",
    'LV_list': "StorageCenter/ScLiveVolume",
    'LV_object': "StorageCenter/ScLiveVolume/{instance_ID}",
    'Use_Local_TB': "StorageCenter/ScLiveVolume/{instance_ID}/UseLocalTiebreaker",
    'Replic_list': "StorageCenter/ScReplicationProgress",
    'Replic_object': "StorageCenter/ScReplicationProgress/{instance_ID}",
    'ScIscsiFaultDomain_list': "StorageCenter/ScIscsiFaultDomain/GetList",
    'ScIscsiFaultDomain_list_update': "StorageCenter/ScIscsiFaultDomain/Rescan",
    'ScIscsiFaultDomain_object': "StorageCenter/ScIscsiFaultDomain/{instance_ID}",
    'ScFaultDomain_list': "StorageCenter/ScFaultDomain/GetList",
    'ScFaultDomain_object': "StorageCenter/ScFaultDomain/{instance_ID}",
    'ScFD_PhysicalPortList': "StorageCenter/ScFaultDomain/{instance_ID}/PhysicalPortList",
    'Virtual_port_list': "StorageCenter/ScFaultDomain/{instance_ID}/VirtualPortList",
    'Port_rebalance_SHD1': "StorageCenter/ScConfiguration/{instance_ID}/RebalancePorts",
    'Port_rebalance_SHD2': "StorageCenter/ScConfiguration/{instance_ID}/RebalancePorts",
    'SC_list_object': "StorageCenter/ScConfiguration/GetList",
    'SC_object': "StorageCenter/ScConfiguration/{instance_ID}",
    'swapRoles': "StorageCenter/ScLiveVolume/{instance_ID}/SwapRoles"
}

path_log_file = os.path.join(os.getcwd(), os.path.normpath("tb_switch_log.log"))
#path_log_file = "/home/developer/tb_switch_log.log"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
f_handler = RotatingFileHandler(path_log_file, mode='a', maxBytes=10*1024*1024, backupCount=2, encoding=None, delay=0)
f_handler.setLevel(logging.INFO)
f_format = logging.Formatter("%(asctime)s - %(name)s: %(levelname)s: %(message)s", "%d.%m.%Y %H:%M:%S")
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)
# __________________________________________END OF INITIALIZATION BLOCK__________________________________________


# ____________________________________________PART OF WORK WITH SNMP_____________________________________________


def snmp_get_value(target_ip, oids, community_string="public", port=161):
    handler = hlapi.getCmd(
        hlapi.SnmpEngine(),
        hlapi.CommunityData(community_string, mpModel=0),
        hlapi.UdpTransportTarget((target_ip, port)),
        hlapi.ContextData(),
        *construct_object_types(oids)
    )
    error_indication, error_status, error_index, var_binds = next(handler)
    result = {}
    if not error_indication and not error_status:
        for var_bind in var_binds:
            result[str(var_bind[0])] = type_convert(var_bind[1])
    else:
        print('Got SNMP error: {0}'.format(error_indication))
        return False, result
    return True, result


def construct_object_types(list_of_oids):
    object_types = []
    oids = list(list_of_oids.values())
    for oid in oids:
        object_types.append(hlapi.ObjectType(hlapi.ObjectIdentity(oid)))
    return object_types


def type_convert(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        try:
            return float(value)
        except (ValueError, TypeError):
            try:
                return str(value)
            except (ValueError, TypeError):
                pass
    return value


def ping_result(ip):
    # function return bool value of pings result
    return ping(ip, count=2).success()


def check_state_ups(ups_ip, oids, primary_volume_location):
    global ups_priority
    ups320_2_state_up = False
    ups320_3_state_up = False
    ups420_2_state_up = False
    ups420_3_state_up = False
    ups320_2_state_up = ping_result(ups_ip['320.2'])
    ups320_3_state_up = ping_result(ups_ip['320.3'])
    ups420_2_state_up = ping_result(ups_ip['420.2'])
    ups420_3_state_up = ping_result(ups_ip['420.3'])
    if ups320_2_state_up:
        logger.info("UPS320_2: Ping successful. Try to reading SNMP parameters")
        value = snmp_get_value(ups_ip['320.2'], oids) # return tuple with bool and dictionary
        if value[0]:
            ups320_2_state = value[1]
        else:
            ups320_2_state_up = False  # change defining attribute from "success ping" to "success snmp get request"
            logger.error("UPS320_2: Unsuccessful reading SNMP parameters")
    else:
        logger.warning("UPS320_2 does not respond to ping")
    if ups320_3_state_up:
        logger.info("UPS320_3: Ping successful. Try to reading SNMP parameters")
        value = snmp_get_value(ups_ip['320.3'], oids)
        if value[0]:
            ups320_3_state = value[1]
        else:
            ups320_3_state_up = False # change defining attribute from "success ping" to "success snmp get request"
            logger.error("UPS320_3: Unsuccessful reading SNMP parameters")
    else:
        logger.warning("UPS320_3 does not respond to ping")
    if ups420_2_state_up:
        logger.info("UPS420_2: Ping successful. Try to reading SNMP parameters")
        value = snmp_get_value(ups_ip['420.2'], oids)
        if value[0]:
            ups420_2_state = value[1]
        else:
            ups420_2_state_up = False # change defining attribute from "success ping" to "success snmp get request"
            logger.error("UPS420_2: Unsuccessful reading SNMP parameters")
    else:
        logger.warning("UPS420_2 does not respond to ping")
    if ups420_3_state_up:
        logger.info("UPS420_3: Ping successful. Try to reading SNMP parameters")
        value = snmp_get_value(ups_ip['420.3'], oids)
        if value[0]:
            ups420_3_state = value[1]
        else:
            ups420_3_state_up = False # change defining attribute from "success ping" to "success snmp get request"
            logger.error("UPS420_3: Unsuccessful reading SNMP parameters")
    else:
        logger.warning("UPS420_3 does not respond to ping")
    if ups320_2_state_up is False and ups320_3_state_up is False and ups420_2_state_up is False \
            and ups420_3_state_up is False:
        return False #  EXIT of function
    # --------------------------- FOR UPS IN 320 ROOM ---------------------------
    if ups320_2_state_up is True:
        try:
            if ups320_2_state[oid_list['upsOutputVoltage']] < 200 and \
                    ups320_2_state[oid_list['upsOutputCurrent']] == 0 and \
                    ups320_2_state[oid_list['upsOutputPower']] == 0:
                ups320_2_power_source = "unknown"
            elif ups320_2_state[oid_list['upsInputVoltage1']] > 200 and \
                    ups320_2_state[oid_list['upsInputVoltage2']] > 200 and \
                    ups320_2_state[oid_list['upsInputVoltage3']] > 200 and \
                    ups320_2_state[oid_list['upsBypassCurrent']] == 0 and \
                    ups320_2_state[oid_list['upsBypassPower']] == 0:
                ups320_2_power_source = "mains power"
            elif ups320_2_state[oid_list['upsInputVoltage1']] > 200 and \
                    ups320_2_state[oid_list['upsInputVoltage2']] > 200 and \
                    ups320_2_state[oid_list['upsInputVoltage3']] > 200 and \
                    (ups320_2_state[oid_list['upsBypassCurrent']] > 0 or
                     ups320_2_state[oid_list['upsBypassPower']] > 0):
                ups320_2_power_source = "bypass"
            elif ups320_2_state[oid_list['upsInputVoltage1']] == 0 and \
                    ups320_2_state[oid_list['upsInputVoltage2']] == 0 and \
                    ups320_2_state[oid_list['upsInputVoltage3']] == 0 and \
                    ups320_2_state[oid_list['upsSecondsOnBattery']] > 0 and \
                    ups320_2_state[oid_list['upsBatteryCurrent']] > 0:
                ups320_2_power_source = "battery"
            if ups320_2_power_source != "unknown":
                ups320_2_percent_charge = ups320_2_state[oid_list['upsEstimatedChargeRemaining']]
                ups320_2_minutes_charge = ups320_2_state[oid_list['upsEstimatedMinutesRemaining']]
                ups320_2_battery_status = ups320_2_state[oid_list['upsBatteryStatus']]
                ups320_2_work_on_battery_time = ups320_2_state[oid_list['upsSecondsOnBattery']]
                logger.info("STATE of 320.2:")
                logger.info("Power source: %s", ups320_2_power_source)
                logger.info("Remaining percent of charge: %s", ups320_2_percent_charge)
                logger.info("Remaining time of charge: %s minutes", ups320_2_minutes_charge)
                logger.info("Common battery status: %s", ups320_2_battery_status)
                logger.info("Work time on battery: %s seconds", ups320_2_work_on_battery_time)
            else:
                logger.warning("UPS320.2 connected to external power but his state is OFF despite that ping and "
                               "snmp get request is successful")
                ups320_2_state_up = False
        except KeyError as k:
            logger.error("Problem with keys of dictionary for state of UPS320.2 (%s)", k)
            ups320_2_state_up = False
    if ups320_3_state_up is True:
        try:
            if ups320_3_state[oid_list['upsOutputVoltage']] < 200 and \
                    ups320_3_state[oid_list['upsOutputCurrent']] == 0 and \
                    ups320_3_state[oid_list['upsOutputPower']] == 0:
                ups320_3_power_source = "unknown"
            elif ups320_3_state[oid_list['upsInputVoltage1']] > 200 and \
                    ups320_3_state[oid_list['upsInputVoltage2']] > 200 and \
                    ups320_3_state[oid_list['upsInputVoltage3']] > 200 and \
                    ups320_3_state[oid_list['upsBypassCurrent']] == 0 and \
                    ups320_3_state[oid_list['upsBypassPower']] == 0:
                    ups320_3_power_source = "mains power"
            elif ups320_3_state[oid_list['upsInputVoltage1']] > 200 and \
                    ups320_3_state[oid_list['upsInputVoltage2']] > 200 and \
                    ups320_3_state[oid_list['upsInputVoltage3']] > 200 and \
                    (ups320_3_state[oid_list['upsBypassCurrent']] > 0 or
                     ups320_3_state[oid_list['upsBypassPower']] > 0):
                ups320_3_power_source = "bypass"
            elif ups320_3_state[oid_list['upsInputVoltage1']] == 0 and \
                    ups320_3_state[oid_list['upsInputVoltage2']] == 0 and \
                    ups320_3_state[oid_list['upsInputVoltage3']] == 0 and \
                    ups320_3_state[oid_list['upsSecondsOnBattery']] > 0 and \
                    ups320_3_state[oid_list['upsBatteryCurrent']] > 0:
                ups320_3_power_source = "battery"
            if ups320_3_power_source != "unknown":
                ups320_3_percent_charge = ups320_3_state[oid_list['upsEstimatedChargeRemaining']]
                ups320_3_minutes_charge = ups320_3_state[oid_list['upsEstimatedMinutesRemaining']]
                ups320_3_battery_status = ups320_3_state[oid_list['upsBatteryStatus']]
                ups320_3_work_on_battery_time = ups320_3_state[oid_list['upsSecondsOnBattery']]
                logger.info("STATE of 320.3:")
                logger.info("Power source: %s", ups320_3_power_source)
                logger.info("Remaining percent of charge: %s", ups320_3_percent_charge)
                logger.info("Remaining time of charge: %s minutes", ups320_3_minutes_charge)
                logger.info("Common battery status: %s", ups320_3_battery_status)
                logger.info("Work time on battery: %s seconds", ups320_3_work_on_battery_time)
            else:
                logger.warning("UPS320.3 connected to external power but his state is OFF despite that ping and "
                               "snmp get request is successful")
                ups320_3_state_up = False
        except KeyError as k:
            logger.error("Problem with keys of dictionary for state of UPS320.3 (%s)", k)
            ups320_3_state_up = False
    if ups320_2_state_up is True and ups320_3_state_up is True:
        ups320_count = 2
        if ups320_2_power_source == "mains power" and ups320_3_power_source == "mains power":
            ups320_power_source = "mains power"
            if ups320_2_percent_charge >= ups320_3_percent_charge and ups320_2_battery_status == 2:
                ups320_percent_charge = ups320_2_percent_charge
                ups320_minutes_charge = ups320_2_minutes_charge
                ups320_battery_status = ups320_2_battery_status
                ups320_work_on_battery_time = ups320_2_work_on_battery_time
            elif ups320_3_percent_charge > ups320_2_percent_charge and ups320_3_battery_status == 2:
                ups320_percent_charge = ups320_3_percent_charge
                ups320_minutes_charge = ups320_3_minutes_charge
                ups320_battery_status = ups320_3_battery_status
                ups320_work_on_battery_time = ups320_3_work_on_battery_time
            elif ups320_2_minutes_charge >= 60 and ups320_2_battery_status == 2:
                ups320_percent_charge = ups320_2_percent_charge
                ups320_minutes_charge = ups320_2_minutes_charge
                ups320_battery_status = ups320_2_battery_status
                ups320_work_on_battery_time = ups320_2_work_on_battery_time
            elif ups320_3_minutes_charge > 60 and ups320_3_battery_status == 2:
                ups320_percent_charge = ups320_3_percent_charge
                ups320_minutes_charge = ups320_3_minutes_charge
                ups320_battery_status = ups320_3_battery_status
                ups320_work_on_battery_time = ups320_3_work_on_battery_time
            else:
                # choose any of two 320_2 or 320_3
                ups320_percent_charge = ups320_2_percent_charge
                ups320_minutes_charge = ups320_2_minutes_charge
                ups320_battery_status = ups320_2_battery_status
                ups320_work_on_battery_time = ups320_2_work_on_battery_time
        elif ups320_2_power_source == "mains power" and ups320_3_power_source != "mains power":
            ups320_power_source = "mains power"
            ups320_percent_charge = ups320_2_percent_charge
            ups320_minutes_charge = ups320_2_minutes_charge
            ups320_battery_status = ups320_2_battery_status
            ups320_work_on_battery_time = ups320_2_work_on_battery_time
        elif ups320_2_power_source != "mains power" and ups320_3_power_source == "mains power":
            ups320_power_source = "mains power"
            ups320_percent_charge = ups320_3_percent_charge
            ups320_minutes_charge = ups320_3_minutes_charge
            ups320_battery_status = ups320_3_battery_status
            ups320_work_on_battery_time = ups320_3_work_on_battery_time
        elif ups320_2_power_source != "mains power" and ups320_3_power_source != "mains power":
            if ups320_2_power_source == "battery" and ups320_3_power_source == "battery":
                ups320_power_source = "battery"
                if ups320_2_work_on_battery_time < 5*60 or ups320_3_work_on_battery_time < 5*60:
                    if ups320_2_minutes_charge >= 60:
                        ups320_percent_charge = ups320_2_percent_charge
                        ups320_minutes_charge = ups320_2_minutes_charge
                        ups320_battery_status = ups320_2_battery_status
                        ups320_work_on_battery_time = ups320_2_work_on_battery_time
                    elif ups320_3_minutes_charge >= 60:
                        ups320_percent_charge = ups320_3_percent_charge
                        ups320_minutes_charge = ups320_3_minutes_charge
                        ups320_battery_status = ups320_3_battery_status
                        ups320_work_on_battery_time = ups320_3_work_on_battery_time
                    else:
                        ups320_count = 0
                else:
                    ups320_count = 0
            elif ups320_2_power_source == "battery" and ups320_3_power_source == "bypass":
                ups320_power_source = "bypass"
                ups320_percent_charge = ups320_3_percent_charge
                ups320_minutes_charge = ups320_3_minutes_charge
                ups320_battery_status = ups320_3_battery_status
                ups320_work_on_battery_time = ups320_3_work_on_battery_time
            elif ups320_2_power_source == "bypass" and ups320_3_power_source == "battery":
                ups320_power_source = "bypass"
                ups320_percent_charge = ups320_2_percent_charge
                ups320_minutes_charge = ups320_2_minutes_charge
                ups320_battery_status = ups320_2_battery_status
                ups320_work_on_battery_time = ups320_2_work_on_battery_time
            elif ups320_2_power_source == "bypass" and ups320_3_power_source == "bypass":
                ups320_power_source = "bypass"
    elif ups320_2_state_up is True and ups320_3_state_up is False:
        ups320_count = 1
        ups320_power_source = ups320_2_power_source
        ups320_percent_charge = ups320_2_percent_charge
        ups320_minutes_charge = ups320_2_minutes_charge
        ups320_battery_status = ups320_2_battery_status
        ups320_work_on_battery_time = ups320_2_work_on_battery_time
    elif ups320_2_state_up is False and ups320_3_state_up is True:
        ups320_count = 1
        ups320_power_source = ups320_3_power_source
        ups320_percent_charge = ups320_3_percent_charge
        ups320_minutes_charge = ups320_3_minutes_charge
        ups320_battery_status = ups320_3_battery_status
        ups320_work_on_battery_time = ups320_3_work_on_battery_time
    else:
        ups320_count = 0
    if ups320_count > 0:
        logger.info("STATE of 320 ROOM:")
        logger.info("Power source: %s", ups320_power_source)
        if ups320_power_source != "bypass":
            logger.info("Remaining percent of charge: %s", ups320_percent_charge)
            logger.info("Remaining time of charge: %s minutes", ups320_minutes_charge)
            logger.info("Common battery status: %s", ups320_battery_status)
            logger.info("Work time on battery: %s seconds", ups320_work_on_battery_time)
    # --------------------------- FOR UPS IN 420 ROOM ---------------------------
    if ups420_2_state_up is True:
        try:
            if ups420_2_state[oid_list['upsOutputVoltage']] < 200 and \
                    ups420_2_state[oid_list['upsOutputCurrent']] == 0 and \
                    ups420_2_state[oid_list['upsOutputPower']] == 0:
                ups420_2_power_source = "unknown"
            elif ups420_2_state[oid_list['upsInputVoltage1']] > 200 and \
                    ups420_2_state[oid_list['upsInputVoltage2']] > 200 and \
                    ups420_2_state[oid_list['upsInputVoltage3']] > 200 and \
                    ups420_2_state[oid_list['upsBypassCurrent']] == 0 and \
                    ups420_2_state[oid_list['upsBypassPower']] == 0:
                ups420_2_power_source = "mains power"
            elif ups420_2_state[oid_list['upsInputVoltage1']] > 200 and \
                    ups420_2_state[oid_list['upsInputVoltage2']] > 200 and \
                    ups420_2_state[oid_list['upsInputVoltage3']] > 200 and \
                    (ups420_2_state[oid_list['upsBypassCurrent']] > 0 or
                     ups420_2_state[oid_list['upsBypassPower']] > 0):
                ups420_2_power_source = "bypass"
            elif ups420_2_state[oid_list['upsInputVoltage1']] == 0 and \
                    ups420_2_state[oid_list['upsInputVoltage2']] == 0 and \
                    ups420_2_state[oid_list['upsInputVoltage3']] == 0 and \
                    ups420_2_state[oid_list['upsSecondsOnBattery']] > 0 and \
                    ups420_2_state[oid_list['upsBatteryCurrent']] > 0:
                ups420_2_power_source = "battery"
            if ups420_2_power_source != "unknown":
                ups420_2_percent_charge = ups420_2_state[oid_list['upsEstimatedChargeRemaining']]
                ups420_2_minutes_charge = ups420_2_state[oid_list['upsEstimatedMinutesRemaining']]
                ups420_2_battery_status = ups420_2_state[oid_list['upsBatteryStatus']]
                ups420_2_work_on_battery_time = ups420_2_state[oid_list['upsSecondsOnBattery']]
                logger.info("STATE of 420.2:")
                logger.info("Power source: %s", ups420_2_power_source)
                logger.info("Remaining percent of charge: %s", ups420_2_percent_charge)
                logger.info("Remaining time of charge: %s minutes", ups420_2_minutes_charge)
                logger.info("Common battery status: %s", ups420_2_battery_status)
                logger.info("Work time on battery: %s seconds", ups420_2_work_on_battery_time)
            else:
                logger.warning("UPS420.2 connected to external power but his state is OFF despite that ping and "
                               "snmp get request is successful")
                ups420_2_state_up = False
        except KeyError as k:
            logger.error("Problem with key of dictionary for state of UPS420.2 (%s)", k)
            ups420_2_state_up = False
    if ups420_3_state_up is True:
        try:
            if ups420_3_state[oid_list['upsOutputVoltage']] < 200 and \
                    ups420_3_state[oid_list['upsOutputCurrent']] == 0 and \
                    ups420_3_state[oid_list['upsOutputPower']] == 0:
                ups420_3_power_source = "unknown"
            elif ups420_3_state[oid_list['upsInputVoltage1']] > 200 and \
                    ups420_3_state[oid_list['upsInputVoltage2']] > 200 and \
                    ups420_3_state[oid_list['upsInputVoltage3']] > 200 and \
                    ups420_3_state[oid_list['upsBypassCurrent']] == 0 and \
                    ups420_3_state[oid_list['upsBypassPower']] == 0:
                ups420_3_power_source = "mains power"
            elif ups420_3_state[oid_list['upsInputVoltage1']] > 200 and \
                    ups420_3_state[oid_list['upsInputVoltage2']] > 200 and \
                    ups420_3_state[oid_list['upsInputVoltage3']] > 200 and \
                    (ups420_3_state[oid_list['upsBypassCurrent']] > 0 or
                     ups420_3_state[oid_list['upsBypassPower']] > 0):
                ups420_3_power_source = "bypass"
            elif ups420_3_state[oid_list['upsInputVoltage1']] == 0 and \
                    ups420_3_state[oid_list['upsInputVoltage2']] == 0 and \
                    ups420_3_state[oid_list['upsInputVoltage3']] == 0 and \
                    ups420_3_state[oid_list['upsSecondsOnBattery']] > 0 and \
                    ups420_3_state[oid_list['upsBatteryCurrent']] > 0:
                ups420_3_power_source = "battery"
            if ups420_3_power_source != "unknown":
                ups420_3_percent_charge = ups420_3_state[oid_list['upsEstimatedChargeRemaining']]
                ups420_3_minutes_charge = ups420_3_state[oid_list['upsEstimatedMinutesRemaining']]
                ups420_3_battery_status = ups420_3_state[oid_list['upsBatteryStatus']]
                ups420_3_work_on_battery_time = ups420_3_state[oid_list['upsSecondsOnBattery']]
                logger.info("STATE of 420.3:")
                logger.info("Power source: %s", ups420_3_power_source)
                logger.info("Remaining percent of charge: %s", ups420_3_percent_charge)
                logger.info("Remaining time of charge: %s minutes", ups420_3_minutes_charge)
                logger.info("Common battery status: %s", ups420_3_battery_status)
                logger.info("Work time on battery: %s seconds", ups420_3_work_on_battery_time)
            else:
                logger.warning("UPS420.3 connected to external power but his state is OFF despite that ping and "
                               "snmp get request is successful")
                ups420_3_state_up = False
        except KeyError as k:
            logger.error("Problem with dictionary for state of UPS420.3 (%s)", k)
            ups420_3_state_up = False
    if ups420_2_state_up is True and ups420_3_state_up is True:
        ups420_count = 2
        if ups420_2_power_source == "mains power" and ups420_3_power_source == "mains power":
            ups420_power_source = "mains power"
            if ups420_2_percent_charge >= ups420_3_percent_charge and ups420_2_battery_status == 2:
                ups420_percent_charge = ups420_2_percent_charge
                ups420_minutes_charge = ups420_2_minutes_charge
                ups420_battery_status = ups420_2_battery_status
                ups420_work_on_battery_time = ups420_2_work_on_battery_time
            elif ups420_3_percent_charge > ups420_2_percent_charge and ups420_3_battery_status == 2:
                ups420_percent_charge = ups420_3_percent_charge
                ups420_minutes_charge = ups420_3_minutes_charge
                ups420_battery_status = ups420_3_battery_status
                ups420_work_on_battery_time = ups420_3_work_on_battery_time
            elif ups420_2_minutes_charge >= 60 and ups420_2_battery_status == 2:
                ups420_percent_charge = ups420_2_percent_charge
                ups420_minutes_charge = ups420_2_minutes_charge
                ups420_battery_status = ups420_2_battery_status
                ups420_work_on_battery_time = ups420_2_work_on_battery_time
            elif ups420_3_minutes_charge > 60 and ups420_3_battery_status == 2:
                ups420_percent_charge = ups420_3_percent_charge
                ups420_minutes_charge = ups420_3_minutes_charge
                ups420_battery_status = ups420_3_battery_status
                ups420_work_on_battery_time = ups420_3_work_on_battery_time
            else:
                # choose any of two 420_2 or 420_3
                ups420_percent_charge = ups420_2_percent_charge
                ups420_minutes_charge = ups420_2_minutes_charge
                ups420_battery_status = ups420_2_battery_status
                ups420_work_on_battery_time = ups420_2_work_on_battery_time
        elif ups420_2_power_source == "mains power" and ups420_3_power_source != "mains power":
            ups420_power_source = "mains power"
            ups420_percent_charge = ups420_2_percent_charge
            ups420_minutes_charge = ups420_2_minutes_charge
            ups420_battery_status = ups420_2_battery_status
            ups420_work_on_battery_time = ups420_2_work_on_battery_time
        elif ups420_2_power_source != "mains power" and ups420_3_power_source == "mains power":
            ups420_power_source = "mains power"
            ups420_percent_charge = ups420_3_percent_charge
            ups420_minutes_charge = ups420_3_minutes_charge
            ups420_battery_status = ups420_3_battery_status
            ups420_work_on_battery_time = ups420_3_work_on_battery_time
        elif ups420_2_power_source != "mains power" and ups420_3_power_source != "mains power":
            if ups420_2_power_source == "battery" and ups420_3_power_source == "battery":
                ups420_power_source = "battery"
                if ups420_2_work_on_battery_time < 5*60 or ups420_3_work_on_battery_time < 5*60:
                    if ups420_2_minutes_charge >= 60:
                        ups420_percent_charge = ups420_2_percent_charge
                        ups420_minutes_charge = ups420_2_minutes_charge
                        ups420_battery_status = ups420_2_battery_status
                        ups420_work_on_battery_time = ups420_2_work_on_battery_time
                    elif ups420_3_minutes_charge >= 60:
                        ups420_percent_charge = ups420_3_percent_charge
                        ups420_minutes_charge = ups420_3_minutes_charge
                        ups420_battery_status = ups420_3_battery_status
                        ups420_work_on_battery_time = ups420_3_work_on_battery_time
                    else:
                        ups420_count = 0
                else:
                    ups420_count = 0
            elif ups420_2_power_source == "battery" and ups420_3_power_source == "bypass":
                ups420_power_source = "bypass"
                ups420_percent_charge = ups420_3_percent_charge
                ups420_minutes_charge = ups420_3_minutes_charge
                ups420_battery_status = ups420_3_battery_status
                ups420_work_on_battery_time = ups420_3_work_on_battery_time
            elif ups420_2_power_source == "bypass" and ups420_3_power_source == "battery":
                ups420_power_source = "bypass"
                ups420_percent_charge = ups420_2_percent_charge
                ups420_minutes_charge = ups420_2_minutes_charge
                ups420_battery_status = ups420_2_battery_status
                ups420_work_on_battery_time = ups420_2_work_on_battery_time
            elif ups420_2_power_source == "bypass" and ups420_3_power_source == "bypass":
                ups420_power_source = "bypass"
    elif ups420_2_state_up is True and ups420_3_state_up is False:
        ups420_count = 1
        ups420_power_source = ups420_2_power_source
        ups420_percent_charge = ups420_2_percent_charge
        ups420_minutes_charge = ups420_2_minutes_charge
        ups420_battery_status = ups420_2_battery_status
        ups420_work_on_battery_time = ups420_2_work_on_battery_time
    elif ups420_2_state_up is False and ups420_3_state_up is True:
        ups420_count = 1
        ups420_power_source = ups420_3_power_source
        ups420_percent_charge = ups420_3_percent_charge
        ups420_minutes_charge = ups420_3_minutes_charge
        ups420_battery_status = ups420_3_battery_status
        ups420_work_on_battery_time = ups420_3_work_on_battery_time
    else:
        ups420_count = 0
    if ups420_count > 0:
        logger.info("STATE of 420 ROOM:")
        logger.info("Power source: %s", ups420_power_source)
        if ups420_power_source != "bypass":
            logger.info("Remaining percent of charge: %s", ups420_percent_charge)
            logger.info("Remaining time of charge: %s minutes", ups420_minutes_charge)
            logger.info("Common battery status: %s", ups420_battery_status)
            logger.info("Work time on battery: %s seconds", ups420_work_on_battery_time)
    # --------------------------- 320 AND 420 ROOM COMPARISON ---------------------------
    if ups320_count > 0 and ups420_count > 0:
        if ups320_count == 2 and ups420_count == 1 and ups320_2_power_source == "main power" and \
                ups320_3_power_source == "main power":
            ups_priority = "320"
        elif ups420_count == 2 and ups320_count == 1 and ups420_2_power_source == "main power" and \
                ups420_3_power_source == "main power":
            ups_priority = "320"
        else:
            if ups320_power_source == "mains power" and ups420_power_source == "mains power":
                if primary_volume_location == "SHD1":
                    if ups320_minutes_charge >= 60 and ups320_battery_status == 2:
                        ups_priority = "320"
                    elif ups320_percent_charge > ups420_percent_charge and ups320_battery_status == 2:
                        ups_priority = "320"
                    elif ups320_battery_status == 2 and ups420_battery_status != 2:
                        ups_priority = "320"
                    else:
                        ups_priority = "420"
                elif primary_volume_location == "SHD2":
                    if ups420_minutes_charge >= 60 and ups420_battery_status == 2:
                        ups_priority = "420"
                    elif ups420_percent_charge > ups320_percent_charge and ups420_battery_status == 2:
                        ups_priority = "420"
                    elif ups420_battery_status == 2 and ups320_battery_status != 2:
                        ups_priority = "420"
                    else:
                        ups_priority = "320"
                else:
                    ups_priority = "Unknown"
            elif ups320_power_source == "mains power" and ups420_power_source != "mains power":
                ups_priority = "320"
            elif ups320_power_source != "mains power" and ups420_power_source == "mains power":
                ups_priority = "420"
            elif ups320_power_source != "mains power" and ups420_power_source != "mains power":
                if ups320_power_source == "battery" and ups420_power_source == "battery":
                    if primary_volume_location == "SHD1":
                        if ups320_work_on_battery_time <= ups420_work_on_battery_time and ups320_minutes_charge > 30:
                            ups_priority = "320"
                        else:
                            ups_priority = "420"
                    elif primary_volume_location == "SHD2":
                        if ups420_work_on_battery_time <= ups320_work_on_battery_time and ups420_minutes_charge > 30:
                            ups_priority = "420"
                        else:
                            ups_priority = "320"
                    else:
                        ups_priority = "Unknown"
                elif ups320_power_source == "bypass" and ups420_power_source == "battery":
                        ups_priority = "320"
                elif ups320_power_source == "battery" and ups420_power_source == "bypass":
                        ups_priority = "420"
                elif ups320_power_source == "bypass" and ups420_power_source == "bypass":
                    if primary_volume_location == "SHD1":
                        ups_priority = "320"
                    elif primary_volume_location == "SHD2":
                        ups_priority = "420"
                    else:
                        ups_priority = "Unknown"
    elif ups320_count > 0 and ups420_count == 0:
        ups_priority = "320"
    elif ups320_count == 0 and ups420_count > 0:
        ups_priority = "420"
    else:
        ups_priority = "Unknown"
    # --------------------------- RETURN PART ---------------------------
    if primary_volume_location == "SHD1" and ups_priority == "320":
        return True  # recommendation for swaping role
    elif primary_volume_location == "SHD2" and ups_priority == "420":
        return True  # recommendation for swaping role
    else:
        return False  # swap of role did not recommended


#  __________________________________PART OF WORK WITH DATA COLLECTOR USING REST API__________________________________


def login():
    global session
    global previous_response_successful
    session = requests.session()
    session.auth = (user_name, password)
    request_url = base_url + url_dict['login']
    logger.info("Request URL: %s", request_url)
    try:
        response = session.post(request_url, headers=header, verify=verify_cert)
    except requests.exceptions.RequestException:
        previous_response_successful = False
        logger.error("Unexpected problem with request, probably DC infrastructure not up yet")
        return False
    try:
        response.raise_for_status()  # catch error code (4xx or 5xx)
        if response.status_code == 200 and response.headers.get('Connection') == 'keep-alive':
            previous_response_successful = True     # initialize variable for firstly call "connect" function
                                                    # through "execute" function
            logger.info("Login to %s Data Collector with ip: %s is successful. Status code: %s",
                        dc_and_tb_state[current_ip_dc], current_ip_dc, response.status_code)
            return True       # return True if connect good
        else:
            previous_response_successful = False
            logger.info("Login to %s Data Collector with ip: %s is unsuccessful. Status code: %s",
                        dc_and_tb_state[current_ip_dc], current_ip_dc, response.status_code)
            return False
    except requests.exceptions.HTTPError:
        previous_response_successful = False
        logger.error("Login to %s Data Collector with IP: %s is unsuccessful. Status code: %s",
                      dc_and_tb_state[current_ip_dc], current_ip_dc, response.status_code)
        return False


def logout(ip):
    #    LOGOUT from Data Collector (through Dell REST API)
    if ip == current_ip_dc:
        request_url = base_url + url_dict['logout']
    else:
        request_url = f"https://{ip}:{port}/api/rest/" + url_dict['logout']
    logger.info("Request URL: %s", request_url)
    try:
        response = session.post(request_url, {})
        # 204 Status Code expected
        session.close()
    except requests.exceptions.RequestException:        # All exceptions that Requests explicitly raises
        logger.error("Problem with request")
        return
    if ip == current_ip_dc:
        if response.status_code in [200, 201, 202, 203, 204]:
            logger.info("Logout from %s Data Collector with ip: %s is successful. Status code: %s",
                        dc_and_tb_state[current_ip_dc], current_ip_dc, response.status_code)
        else:
            logger.info("Logout from %s Data Collector with ip: %s is unsuccessful. Status code: %s",
                        dc_and_tb_state[current_ip_dc], current_ip_dc, response.status_code)
    else:
        if response.status_code in [200, 201, 202, 203, 204]:
            logger.info("Logout from %s Data Collector with ip: %s is successful. Status code: %s",
                        dc_and_tb_state[ip], ip, response.status_code)
        else:
            logger.info("Logout from %s Data Collector with ip: %s is unsuccessful. Status code: %s",
                        dc_and_tb_state[ip], ip, response.status_code)
    try:
        http_body = json.loads(response.json())
        logger.info("Call function for reading HTTP-body")
        http_body_read(http_body, url_dict['logout'])
    except json.decoder.JSONDecodeError or ValueError:
        logger.error("Logout operation: Something problems with reading json")


def connect(extension_url, method):
    request_url = base_url + extension_url
    payload = {}
    global previous_response_successful
    if method == "get":
        logger.info("Request URL: %s", request_url)
        try:
            response = session.get(request_url, data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
                                   verify=False,
                                   headers=header)
        except requests.exceptions.RequestException:        # All exceptions that Requests explicitly raises
            previous_response_successful = False
            logger.error("Unexpected problem with request, probably DC infrastructure not up yet")
            return
        try:
            response.raise_for_status()  # catch error code (4xx or 5xx)
            if response.status_code in [200, 202, 204]:
                logger.info("Successful response for URL: %s", request_url)
                if len(response.text) > 5:  # Cut off empty json
                    http_body = json.loads(response.text)  # convert json string to python dictionary type
                    logger.info("Call function for reading HTTP-body")
                    if http_body_read(http_body, extension_url):
                        previous_response_successful = True
                    else:
                        previous_response_successful = False
                    return          # exit from function without return arguments
                else:
                    previous_response_successful = True
                    return
        except requests.exceptions.HTTPError:
            logger.error("Bad status code for http request 4xx or 5xx")
            logger.error(response.text)
        except json.decoder.JSONDecodeError or ValueError:
            logger.error("Something problems with reading json")
            previous_response_successful = False
            return

    elif method == "post":
        logger.info("Request URL: %s", request_url)
        try:
            response = session.post(request_url, data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
                                    verify=False,
                                    headers=header)
        except requests.exceptions.RequestException:        # All exceptions that Requests explicitly raises
            previous_response_successful = False
            logger.error("Unexpected problem with request, probably DC infrastructure not up yet")
            return
        try:
            response.raise_for_status()  # catch error code (4xx or 5xx)
            if response.status_code in [200, 202, 204]:  # Cut off empty json
                logger.info("Successful response for URL: %s", request_url)
                if len(response.text) > 5:
                    http_body = json.loads(response.text)  # convert json string to python dictionary type
                    logger.info("Call function for reading HTTP-body")
                    if http_body_read(http_body, extension_url):
                        previous_response_successful = True
                    else:
                        previous_response_successful = False
                    return          # exit from function without return arguments
                else:
                    previous_response_successful = True
                    return
        except requests.exceptions.HTTPError:
            logger.error("Bad status code for http request 4xx or 5xx")
            logger.error(response.text)
        except json.decoder.JSONDecodeError or ValueError:
            logger.error("Something problems with reading json")
            previous_response_successful = False
            return
    # HTTP-response unsuccessful:
    logger.info("HTTP response unsuccessful. Status code: %s. Length of responses text %s",
                response.status_code, len(response.text))
    previous_response_successful = False


def http_body_read(body_http, url):
    global sc_list
    global lv_object
    global lv_object_after_change
    global replic_object
    global volume_and_SC_roles
    global SHD1_port_rebalance_needed
    global SHD2_port_rebalance_needed
    if url_dict['login'] == url:
        pass

    elif url_dict['api_connect'] == url:
        logger.info("Reading HTTP-body for request URL: %s", url_dict['api_connect'])
        connect_instanceId = body_http['instanceId']
        connect_status = body_http['connected']
        connect_hostname = body_http['hostName']
        if connect_hostname == primary_hostname or connect_hostname == remote_hostname:
            url_dict['SC_array_list'] = url_dict['SC_array_list'].replace('{instance_ID}', connect_instanceId)
        else:
            logger.error("%s and %s don't found in data collector configuration", primary_hostname, remote_hostname)
            return False

    elif url_dict['SC_array_list'] == url:
        # get the list of Storage Centers (All SC Series array managed by DSM Data Collector)
        logger.info("Reading HTTP-body for request URL: %s", url_dict['SC_array_list'])
        sc_list = {}  # python dictionary which contains list of storage centers
        try:
            if len(body_http) > 0:
                logger.info("%s\t%s\t%s\t%s\t%s\t%s", "Name of SC", "Serial Number", "ID of SC", "IP address of SC",
                            "Connected to DC", "Status of SC")
                for i in range(len(body_http)):
                    logger.info("%s\t\t\t%s\t\t\t%s\t\t%s\t\t\t%s\t\t\t%s", body_http[i]['name'],
                                body_http[i]['scSerialNumber'], body_http[i]['instanceId'],
                                body_http[i]['hostOrIpAddress'], body_http[i]['connected'],
                                body_http[i]['status'])
                    sc_list[body_http[i]['name']] = {}
                    sc_list[body_http[i]['name']]['instanceId'] = body_http[i]['instanceId']    # ID of each Storage Centers(SC4020)
                    sc_list[body_http[i]['name']]['hostOrIpAddress'] = body_http[i]['hostOrIpAddress']
                    sc_list[body_http[i]['name']]['connected'] = body_http[i]['connected']
                    sc_list[body_http[i]['name']]['status'] = body_http[i]['status']
                    sc_list[body_http[i]['name']]['portsBalanced'] = body_http[i]['portsBalanced']
                if 'SHD1' in sc_list.keys() and 'SHD2' in sc_list.keys():
                    if sc_list['SHD1']['status'] != "Down" and sc_list['SHD2']['status'] != "Down":
                        logger.info("%s Data Collector connected to all Storage Centers: SHD1 and SHD2",
                                    dc_and_tb_state[current_ip_dc])
                    elif sc_list['SHD1']['status'] != "Down" and sc_list['SHD2']['status'] == "Down":
                        logger.info("%s Data Collector connected to only SHD1",
                                    dc_and_tb_state[current_ip_dc])
                    elif sc_list['SHD1']['status'] == "Down" and sc_list['SHD2']['status'] != "Down":
                        logger.info("%s Data Collector connected to only SHD2",
                                    dc_and_tb_state[current_ip_dc])
                elif 'SHD1' in sc_list.keys() and 'SHD2' not in sc_list.keys():
                    logger.warning("%s Data Collector connected to only SHD1", dc_and_tb_state[current_ip_dc])
                elif 'SHD2' in sc_list.keys() and 'SHD1' not in sc_list.keys():
                    logger.warning("%s Data Collector connected to only SHD2", dc_and_tb_state[current_ip_dc])
                else:
                    logger.error("%s Data Collector have not connected to any storage centers", dc_and_tb_state[current_ip_dc])
                    return False
            else:
                logger.error("%s Data Collector have not connected to any storage centers", dc_and_tb_state[current_ip_dc])
                return False
        except KeyError:
            logger.error("Problem with mismatch of parameter list between received from DC and declared in API documentation")
            return False

    elif url_dict['LV_list'] == url:
        # get list of Live Volumes
        logger.info("Reading HTTP-body for request URL: %s", url_dict['LV_list'])
        lv_list = {}
        try:
            logger.info("%s\t\t\t\t\t\t\t%s\t\t\t%s", "instanceName", "instanceId", "objectType")
            for i in range(len(body_http)):
                logger.info("%s\t\t%s\t\t%s", body_http[i]['instanceName'], body_http[i]['instanceId'],
                            body_http[i]['objectType'])
                if body_http[i]['instanceName'] == "Live Volume of " + name_volume['first']:
                    name_LV_object = "Live Volume of " + name_volume['first']
                elif body_http[i]['instanceName'] == "Live Volume of " + name_volume['second']:
                    name_LV_object = "Live Volume of " + name_volume['second']
                else:
                    name_LV_object = "Unknown"
                if body_http[i]['instanceName'] == name_LV_object:
                    lv_list[name_LV_object] = {}
                    # ID of Live Volume with name "Live Volume of HistorianVolume1(2)"
                    lv_list[name_LV_object]['instanceId'] = body_http[i]['instanceId']
                    lv_list[name_LV_object]['objectType'] = body_http[i]['objectType']
            if name_LV_object in lv_list.keys():
                lv_id = lv_list[name_LV_object]['instanceId']
                url_dict['LV_object'] = url_dict['LV_object'].replace('{instance_ID}', lv_id)       # prepare url for HTTP-request
                url_dict['Use_Local_TB'] = url_dict['Use_Local_TB'].replace('{instance_ID}', lv_id) # prepare url for HTTP-request
                url_dict['swapRoles'] = url_dict['swapRoles'].replace('{instance_ID}', lv_id)       # prepare url for HTTP-request
            elif SHD1_state_up or SHD2_state_up is not True:
                lv_id = body_http[0]['instanceId']
                url_dict['LV_object'] = url_dict['LV_object'].replace('{instance_ID}', lv_id)        # prepare url for HTTP-request
                url_dict['Use_Local_TB'] = url_dict['Use_Local_TB'].replace('{instance_ID}', lv_id)  # prepare url for HTTP-request
            else:
                logger.error("%s and %s was not found in configuration of %s Data Collector. %s was not found in configuration",
                             "Live Volume of " + name_volume['first'], "Live Volume of " + name_volume['first'],
                             dc_and_tb_state[current_ip_dc], name_LV_object)
                return False   # for going to new iteration
        except KeyError:
            logger.error("Problem with mismatch of parameter list between received from DC and declared in API documentation")
            return False

    elif url_dict['LV_object'] == url:
        # get live volume (object) with name "Live Volume of HistorianVolume"
        logger.info("Reading HTTP-body for request URL: %s", url_dict['LV_object'])
        lv_state_for_object = [
            ["primaryStatus", "expect 'Up'"],                   #  Primary volume status (expect "Up")
            ["secondaryStatus", "expect 'Up'"],                 #  Secondary volume status (expect "Up")
            ["secondaryRole", "expect 'Secondary'"],            #  (expect "Secondary")
            ["primaryRole", "expect 'Primary'"],                #  (expect "Primary")
            ["primaryPeerState", "expect 'Connected'"],         #  type (expect "Connected")
            ["secondaryPeerState", "expect 'Connected'"],       #  type (expect "Connected")
            ["swappingRoles", "expect false"],                  #  type (expect false)
            ["managedReplicationsAllowed", "expect true"],       #  type (expect true)
            ["managingReplications", "expect false"],            #  type (expect false)
            ["swapRolesAutomaticallyEnabled", "expect true"],    #  type (expect true)
            ["failoverAutomaticallyEnabled", "expect true"],     #  type (expect true)
            ["restoreAutomaticallyEnabled", "expect true"],      #  type (expect true)
            ["localTiebreaker", "expect true"],                  #  type (expect true)
            ["failoverState", "expect 'Protected'"],             #  type (expect "Protected")
            ["primaryToTiebreakerConnectivity", "expect 'Up'"],  #  type (expect "Up")
            ["secondaryToTiebreakerConnectivity", "expect 'Up'"],#  type (expect "Up")
            ["aluaOptimized", "expect true"],                    #  type (expect true)
            ["replicationFound", "expect true"],                 #  type (expect true)
            ["replicationState", "expect 'Up'"],                 #  state of replication (expect "Up")
            ["replicationStateMessage", "expect '-'"],
            ["type", "expect 'Synchronous'"],                    #  type of Replication (expect "Synchronous")
            ["syncMode", "expect 'HighAvailability'"],           #  mode of synchronization (expect "HighAvailability")
            ["syncStatus", "expect 'Current'"],                  #  status of synchronization (expect "Current")
            ["status", "expect 'Up'"],                           # status of Live Volume
            ["instanceId", "expect '---'"],                      # ID of Live Volume
            ["instanceName", "expect '---'"],                    # Name of Live Volume
            ["automaticallySwapRoles", "expect true"],           #  type (expect true)
            ["primarySwapRoleState", "expect 'NotSwapping'"],    #  type (expect "NotSwapping")
            ["secondarySwapRoleState", "expect 'NotSwapping'"]   #  type (expect "NotSwapping")
        ]
        if SHD1_state_up is False or SHD2_state_up is False or body_http["primaryStatus"] != "Up" or  \
                body_http["secondaryStatus"] != "Up":      # after fail this keys is not return
            lv_state_for_object.remove(["type", "expect 'Synchronous'"])
            lv_state_for_object.remove(["syncMode", "expect 'HighAvailability'"])
            lv_state_for_object.remove(["syncStatus", "expect 'Current'"])
        max_length_1 = 1
        max_length_2 = 1
        try:
            for i in range(len(lv_state_for_object)):
                if max_length_1 < len(lv_state_for_object[i][0]):
                    max_length_1 = len(lv_state_for_object[i][0])
                if type(body_http[lv_state_for_object[i][0]]) == str:
                    if max_length_2 < len(body_http[lv_state_for_object[i][0]]):
                        max_length_2 = len(body_http[lv_state_for_object[i][0]])
            lv_object = {}
            volume_and_SC_roles = {}
            logger.info("STATE of Live Volume for %s Data Collector:", dc_and_tb_state[current_ip_dc])
            for i in range(len(lv_state_for_object)):
                lv_object[lv_state_for_object[i][0]] = body_http[lv_state_for_object[i][0]]
                if type(body_http[lv_state_for_object[i][0]]) == str:
                    logger.info("%s\t%s\t%s", lv_state_for_object[i][0].ljust(max_length_1),
                                body_http[lv_state_for_object[i][0]].ljust(max_length_2), lv_state_for_object[i][1])
                else:
                    logger.info("%s\t%s\t%s", lv_state_for_object[i][0].ljust(max_length_1),
                                f"{body_http[lv_state_for_object[i][0]]}".ljust(max_length_2), lv_state_for_object[i][1])
            if SHD1_state_up is True and SHD2_state_up is True:
                if sc_list['SHD1']['connected'] is True and sc_list['SHD2']['connected'] is True \
                        and sc_list['SHD1']['status'] != "Down" and sc_list['SHD2']['status'] != "Down":
                    volume_and_SC_roles['primaryStorageCenter'] = body_http['primaryStorageCenter']['instanceName']     # "SHD1" or "SHD2"
                    volume_and_SC_roles['secondaryStorageCenter'] = body_http['secondaryStorageCenter']['instanceName'] # "SHD1" or "SHD2"
                    volume_and_SC_roles['primaryVolume'] = body_http['primaryVolume']['instanceName']      # "HistorianVolume1" or "HistorianVolume2"
                    volume_and_SC_roles['secondaryVolume'] = body_http['secondaryVolume']['instanceName']  # "HistorianVolume1" or "HistorianVolume2"
                else:
                    volume_and_SC_roles['primaryStorageCenter'] = body_http['primaryStorageCenter']['instanceName']     # "SHD1" or "SHD2"
                    volume_and_SC_roles['secondaryStorageCenter'] = body_http['secondaryStorageCenter']['instanceName'] # "SHD1" or "SHD2"
            else:
                volume_and_SC_roles['primaryStorageCenter'] = body_http['primaryStorageCenter']['instanceName']         # "SHD1" or "SHD2"
                volume_and_SC_roles['secondaryStorageCenter'] = body_http['secondaryStorageCenter']['instanceName']     # "SHD1" or "SHD2"
        except KeyError:
            logger.error("Problem with mismatch of parameter list between received from DC and declared in API documentation")
            return False

    elif url_dict['Replic_list'] == url:
        # get list of replications
        logger.info("Reading HTTP-body for request URL: %s", url_dict['Replic_list'])
        replic_list = {}
        try:
            for i in range(len(body_http)):
                logger.info("%s Data Collector have information about the following replications:", dc_and_tb_state[current_ip_dc])
                logger.info("%s\t\t%s\t\t%s", body_http[i]['instanceName'], body_http[i]['instanceId'],
                            body_http[i]['objectType'])
                if body_http[i]['instanceName'] == "Replication of " + name_volume['first']:
                    name_replic_object = "Replication of " + name_volume['first']
                elif body_http[i]['instanceName'] == "Replication of " + name_volume['second']:
                    name_replic_object = "Replication of " + name_volume['second']
                else:
                    name_replic_object = "Unknown"
                if body_http[i]['instanceName'] == name_replic_object:
                    replic_list[name_replic_object] = {}
                    # ID of Replication with name "Replication of HistorianVolume1(2)"
                    replic_list[name_replic_object]['instanceId'] = body_http[i]['instanceId']
                    replic_list[name_replic_object]['objectType'] = body_http[i]['objectType']
            if name_replic_object in replic_list.keys():
                replic_id = replic_list[name_replic_object]['instanceId']
                url_dict['Replic_object'] = url_dict['Replic_object'].replace('{instance_ID}', replic_id)  # prepare url for HTTP-request
            elif SHD1_state_up is False or SHD2_state_up is False:
                replic_id = body_http[0]['instanceId']
                url_dict['Replic_object'] = url_dict['Replic_object'].replace('{instance_ID}', replic_id)  # prepare url for HTTP-request
            else:
                logger.error("Replications: '%s' and '%s' was not found in configuration of %s Data Collector. "
                             "'%s' replication was not found",
                             "Replication of "+name_volume['first'], "Replication of "+name_volume['second'],
                             name_replic_object, dc_and_tb_state[current_ip_dc])
        except KeyError:
            logger.error("Problem with mismatch of parameter list between received from DC and declared in API documentation")

    elif url_dict['Replic_object'] == url:
        # get object of replication
        logger.info("Reading HTTP-body for request URL: %s", url_dict['Replic_object'])
        replic_state_for_object = [
            'instanceName',
            'instanceId',
            'objectType',
            'percentComplete',
            'synced',
            'state',
            'totalSize',
            'scName'
        ]
        max_length_1 = 1
        max_length_2 = 1
        try:
            for i in range(len(replic_state_for_object)):
                if max_length_1 < len(replic_state_for_object[i]):
                    max_length_1 = len(replic_state_for_object[i])
                if type(body_http[replic_state_for_object[i]]) == str:
                    if max_length_2 < len(body_http[replic_state_for_object[i]]):
                        max_length_2 = len(body_http[replic_state_for_object[i]])
            replic_object = {}
            logger.info("STATE of Replication:")
            for i in range(len(replic_state_for_object)):
                replic_object[replic_state_for_object[i]] = body_http[replic_state_for_object[i]]
                if type(body_http[replic_state_for_object[i]]) == str:
                    logger.info("%s\t%s", replic_state_for_object[i].ljust(max_length_1),
                                body_http[replic_state_for_object[i]].ljust(max_length_2))
                else:
                    logger.info("%s\t%s", replic_state_for_object[i].ljust(max_length_1),
                                f"{body_http[replic_state_for_object[i]]}".ljust(max_length_2))
        except KeyError:
            logger.error("Problem with mismatch of parameter list between received from DC and declared in API documentation")

    elif url_dict['Use_Local_TB'] == url:
        # change state of Local Tiebreaker and get new state of live volume (object) after change
        # with name "Live Volume of HistorianVolume"
        logger.info("Reading HTTP-body for request URL: %s", url_dict['Use_Local_TB'])
        lv_state_for_object_after_change = [
            ["primaryStatus", "expect 'Up'"],                   #  Primary volume status (expect "Up")
            ["secondaryStatus", "expect 'Up'"],                 #  Secondary volume status (expect "Up")
            ["secondaryRole", "expect 'Secondary'"],            #  (expect "Secondary")
            ["primaryRole", "expect 'Primary'"],                #  (expect "Primary")
            ["primaryPeerState", "expect 'Connected'"],         #  type (expect "Connected")
            ["secondaryPeerState", "expect 'Connected'"],       #  type (expect "Connected")
            ["swapRolesAutomaticallyEnabled", "expect true"],    #  type (expect true)
            ["failoverAutomaticallyEnabled", "expect true"],     #  type (expect true)
            ["restoreAutomaticallyEnabled", "expect true"],      #  type (expect true)
            ["localTiebreaker", "expect true"],                  #  type (expect true)
            ["failoverState", "expect 'Protected'"],             #  type (expect "Protected")
            ["primaryToTiebreakerConnectivity", "expect 'Up'"],  #  type (expect "Up")
            ["secondaryToTiebreakerConnectivity", "expect 'Up'"],#  type (expect "Up")
            ["replicationFound", "expect true"],                 #  type (expect true)
            ["replicationState", "expect 'Up'"],                 #  state of replication (expect "Up")
            ["type", "expect 'Synchronous'"],                    #  type of Replication (expect "Synchronous")
            ["syncMode", "expect 'HighAvailability'"],           #  mode of synchronization (expect "HighAvailability")
            ["syncStatus", "expect 'Current'"],                  #  status of synchronization (expect "Current")
            ["status", "expect '---'"],                          # status of Live Volume
            ["instanceId", "expect '---'"],                      # ID of Live Volume
            ["instanceName", "expect '---'"]                     # Name of Live Volume
        ]
        if SHD1_state_up is False or SHD2_state_up is False or body_http["primaryStatus"] != "Up" or \
                body_http["secondaryStatus"] != "Up":      # after fail this keys is not return
            lv_state_for_object_after_change.remove(["type", "expect 'Synchronous'"])
            lv_state_for_object_after_change.remove(["syncMode", "expect 'HighAvailability'"])
            lv_state_for_object_after_change.remove(["syncStatus", "expect 'Current'"])
        max_length_1 = 1
        max_length_2 = 1
        try:
            for i in range(len(lv_state_for_object_after_change)):
                if max_length_1 < len(lv_state_for_object_after_change[i][0]):
                    max_length_1 = len(lv_state_for_object_after_change[i][0])
                if type(body_http[lv_state_for_object_after_change[i][0]]) == str:
                    if max_length_2 < len(body_http[lv_state_for_object_after_change[i][0]]):
                        max_length_2 = len(body_http[lv_state_for_object_after_change[i][0]])
            logger.info("STATE of Live Volume after apply command 'Use local tiebreaker':")
            lv_object_after_change = {}
            for i in range(len(lv_state_for_object_after_change)):
                lv_object_after_change[lv_state_for_object_after_change[i][0]] = body_http[lv_state_for_object_after_change[i][0]]
                if type(body_http[lv_state_for_object_after_change[i][0]]) == str:
                    logger.info("%s\t%s\t%s", lv_state_for_object_after_change[i][0].ljust(max_length_1),
                                body_http[lv_state_for_object_after_change[i][0]].ljust(max_length_2), lv_state_for_object_after_change[i][1])
                else:
                    logger.info("%s\t%s\t%s", lv_state_for_object_after_change[i][0].ljust(max_length_1),
                                f"{body_http[lv_state_for_object_after_change[i][0]]}".ljust(max_length_2), lv_state_for_object_after_change[i][1])
        except KeyError:
            logger.error("Problem with mismatch of parameter list between received from DC and declared in API documentation")
            return False
        try:
            for i in range(len(lv_state_for_object_after_change)):
                lv_object[lv_state_for_object_after_change[i][0]] = lv_object_after_change[lv_state_for_object_after_change[i][0]]
        except KeyError:
            logger.error("Problem with update value in parameters of live volume after Use_Local_TB")
            return False

    elif url_dict['SC_list_object'] == url:
        # Determine the need for port balancing
        try:
            for i in range(len(body_http)):
                if body_http[i]['scName'] == "SHD1":
                    SHD1_port_rebalance_needed = body_http[i]['portRebalanceNeeded']
                    url_dict['Port_rebalance_SHD1'] = url_dict['Port_rebalance_SHD1'].replace('{instance_ID}', str(body_http[i]['scSerialNumber']))  # prepare url for HTTP-request
                elif body_http[i]['scName'] == "SHD2":
                    SHD2_port_rebalance_needed = body_http[i]['portRebalanceNeeded']
                    url_dict['Port_rebalance_SHD2'] = url_dict['Port_rebalance_SHD2'].replace('{instance_ID}', str(body_http[i]['scSerialNumber']))  # prepare url for HTTP-request
        except KeyError:
            logger.error("Problem with mismatch of parameter list between received from DC and declared in API documentation")

    return True   # common success return


def execute(request_short_url, method='get', ip_dc="default"):
    if request_short_url == "ApiConnection/Login":
        return login()
    elif request_short_url == "ApiConnection/Logout":
        logout(ip_dc)
    else:
        connect(request_short_url, method)


def state_storage_center_determine():
    if previous_response_successful:
        execute(url_dict['api_connect'])
    else:
        return False
    if previous_response_successful:
        execute(url_dict['SC_array_list'])  # get the list of Storage Centers
    else:
        return False
    if previous_response_successful:
        execute(url_dict['LV_list'])  # get the list of Live Volume
    else:
        return False
    if previous_response_successful:
        execute(url_dict['LV_object'])  # get Live Volume object
    else:
        return False
    if previous_response_successful:
        if SHD1_state_up is True and SHD2_state_up is True:
            execute(url_dict['Replic_list'])  # get list of replication objects
    else:
        return False
    if previous_response_successful:
        if SHD1_state_up is True and SHD2_state_up is True:
            execute(url_dict['Replic_object'])  # get replications object
    else:
        return False
    return True


def clean_urls():
    global url_dict
    url_dict = {
        'login': "ApiConnection/Login",
        'logout': "ApiConnection/Logout",
        'api_connect': "ApiConnection/ApiConnection",
        'SC_array_list': "ApiConnection/ApiConnection/{instance_ID}/StorageCenterList",
        'LV_list': "StorageCenter/ScLiveVolume",
        'LV_object': "StorageCenter/ScLiveVolume/{instance_ID}",
        'Use_Local_TB': "StorageCenter/ScLiveVolume/{instance_ID}/UseLocalTiebreaker",
        'Replic_list': "StorageCenter/ScReplicationProgress",
        'Replic_object': "StorageCenter/ScReplicationProgress/{instance_ID}",
        'ScIscsiFaultDomain_list': "StorageCenter/ScIscsiFaultDomain/GetList",
        'ScIscsiFaultDomain_list_update': "StorageCenter/ScIscsiFaultDomain/Rescan",
        'ScIscsiFaultDomain_object': "StorageCenter/ScIscsiFaultDomain/{instance_ID}",
        'ScFaultDomain_list': "StorageCenter/ScFaultDomain/GetList",
        'ScFaultDomain_object': "StorageCenter/ScFaultDomain/{instance_ID}",
        'ScFD_PhysicalPortList': "StorageCenter/ScFaultDomain/{instance_ID}/PhysicalPortList",
        'Virtual_port_list': "StorageCenter/ScFaultDomain/{instance_ID}/VirtualPortList",
        'Port_rebalance_SHD1': "StorageCenter/ScConfiguration/{instance_ID}/RebalancePorts",
        'Port_rebalance_SHD2': "StorageCenter/ScConfiguration/{instance_ID}/RebalancePorts",
        'SC_list_object': "StorageCenter/ScConfiguration/GetList",
        'SC_object': "StorageCenter/ScConfiguration/{instance_ID}",
        'swapRoles': "StorageCenter/ScLiveVolume/{instance_ID}/SwapRoles"
    }


def ping_result(ip):
    # function return bool value of pings result
    return ping(ip, count=2).success()


def check_state_disks_array():
    global SHD1_state_up
    global SHD2_state_up
    if ping_result(SHD1_virtual_ip):
        SHD1_state_up = True
    else:
        logger.warning("SHD1 (disks array SC4020 in 420 room) does not respond to ping")
        SHD1_state_up = False
    if ping_result(SHD2_virtual_ip):
        SHD2_state_up = True
    else:
        logger.warning("SHD2 (disks array SC4020 in 320 room) does not respond to ping")
        SHD2_state_up = False
    if SHD1_state_up or SHD2_state_up is True:
        return True
    else:
        return False


def check_state_data_collector():
    # return True if at least one data collector ping successful
    global dc_primary_state_up
    global dc_remote_state_up
    global tb_active_flag
    global local_tiebreaker_use
    global dc_reboot_detected
    if dc_primary_state_up is True and ping_result(primary_ip) is False:
        time.sleep(30)  # for check if DC just reboot state
        if ping_result(primary_ip) is True:
            dc_reboot_detected = True
            tb_active_flag = False
            logger.info("Detected reboot of %s Data Collector.", dc_and_tb_state[primary_ip])
    if dc_remote_state_up is True and ping_result(remote_ip) is False:
        time.sleep(30)  # for check if DC just reboot state
        if ping_result(remote_ip) is True:
            dc_reboot_detected = True
            tb_active_flag = False
            logger.info("Detected reboot of %s Data Collector.", dc_and_tb_state[remote_ip])
    if ping_result(primary_ip):
        dc_primary_state_up = True
        #logger.info("Ping %s Data Collector successful!", dc_and_tb_state[primary_ip])
        if ping_result(remote_ip):
            dc_remote_state_up = True
            #logger.info("Ping %s Data Collector successful!", dc_and_tb_state[remote_ip])
        else:
            dc_remote_state_up = False
            logger.info("%s Data Collector does not respond to ping!", dc_and_tb_state[remote_ip])
        return True
    elif ping_result(remote_ip):
        dc_primary_state_up = False
        dc_remote_state_up = True
        logger.info("%s Data Collector does not respond to ping!", dc_and_tb_state[primary_ip])
        #logger.info("Ping %s Data Collector successful!", dc_and_tb_state[remote_ip])
        return True
    else:
        dc_primary_state_up = False
        dc_remote_state_up = False
        tb_active_flag = False
        local_tiebreaker_use = dc_and_tb_state["unknown_ip"]
        logger.warning("%s and %s Data Collectors does not respond to ping!", dc_and_tb_state[primary_ip],
                       dc_and_tb_state[remote_ip])
        return False


def connect_to_data_collector(host_ip):
    global base_url
    global dc_primary_state_up_previous
    global dc_remote_state_up_previous
    if host_ip == primary_ip:
        base_url = "https://{}:{}/api/rest/".format(primary_ip, port)
    elif host_ip == remote_ip:
        base_url = "https://{}:{}/api/rest/".format(remote_ip, port)
    if execute(url_dict['login']):
        return True
    else:
        if host_ip == primary_ip and dc_primary_state_up:
            dc_primary_state_up_previous = False  # for repeat iteration after ping Up when infrastructure of data collector is not up yet
        elif host_ip == remote_ip and dc_remote_state_up:
            dc_remote_state_up_previous = False  # for repeat iteration after ping Up when infrastructure of data collector is not up yet
        logger.error("Unsuccessful connection to %s Data Collector (because login operation to "
                       "%s Data Collector failed). Go to new iteration...",
                       dc_and_tb_state[current_ip_dc], dc_and_tb_state[current_ip_dc])
        return False


def tb_switch():
    global tb_active_flag
    global current_ip_dc
    if current_ip_dc == primary_ip:
        if local_tiebreaker_use != "Primary" or tb_active_flag is False:
            logger.info("Call of function 'use_local_tiebreaker' for %s Data Collector", dc_and_tb_state[current_ip_dc])
            use_local_tiebreaker()
        else:
            logger.info("Local Tiebreaker is already active for %s Data Collector", dc_and_tb_state[current_ip_dc])
    elif current_ip_dc == remote_ip:
        if local_tiebreaker_use != "Remote" or tb_active_flag is False:
            logger.info("Call of function 'use_local_tiebreaker' for %s Data Collector", dc_and_tb_state[current_ip_dc])
            use_local_tiebreaker()
        else:
            logger.info("Local Tiebreaker is already active for %s Data Collector", dc_and_tb_state[current_ip_dc])


def use_local_tiebreaker():
    global local_tiebreaker_use
    global tb_active_flag
    if previous_response_successful:
        if lv_object["secondaryStatus"] == "Up" and lv_object["primaryStatus"] == "Up":
            # this block execute when both SC (primary and secondary volume) is Up
            if lv_object["localTiebreaker"] and lv_object["failoverState"] != "Unprotected":        # do not need to change state of Local Tiebreaker
                logger.info("Local Tiebreaker is already active for %s Data Collector", dc_and_tb_state[current_ip_dc])
                local_tiebreaker_use = dc_and_tb_state[current_ip_dc]
                tb_active_flag = True
            elif lv_object["localTiebreaker"] is False:
                logger.info("Changing to active state of Local Tiebreaker for %s Data Collector...",
                            dc_and_tb_state[current_ip_dc])
                execute(url_dict['Use_Local_TB'], 'post')  # change state of Local Tiebreaker
                if previous_response_successful:
                    if lv_object_after_change["localTiebreaker"] and \
                            lv_object_after_change["failoverState"] != "Unprotected":
                        logger.info("Local Tiebreaker changed state to active for %s Data Collector",
                                    dc_and_tb_state[current_ip_dc])
                        local_tiebreaker_use = dc_and_tb_state[current_ip_dc]
                        tb_active_flag = True
                    else:
                        time.sleep(60)                      # time delay for repeat polling of Live Volume
                        logger.info("Repeated polling of state Live Volume for %s Data Collector", dc_and_tb_state[current_ip_dc])
                        execute(url_dict['LV_object'])      # get Live Volume object
                        if previous_response_successful:
                            if lv_object["localTiebreaker"] and lv_object["failoverState"] != "Unprotected":
                                logger.info("Local Tiebreaker successfully changed state to active for %s Data Collector",
                                            dc_and_tb_state[current_ip_dc])
                                local_tiebreaker_use = dc_and_tb_state[current_ip_dc]
                                tb_active_flag = True
                            else:
                                logger.warning("Local Tiebreaker can not changed state to active for %s Data Collector",
                                               dc_and_tb_state[current_ip_dc])
                                local_tiebreaker_use = dc_and_tb_state["unknown_ip"]
                                tb_active_flag = False
                                return False
                        else:
                            logger.warning("State of Live Volume is not normal after change state of tiebreaker for %s "
                                           "Data Collector. (previous_response_successful = False) Go to new iteration...",
                                           dc_and_tb_state[current_ip_dc])
                            local_tiebreaker_use = dc_and_tb_state["unknown_ip"]
                            tb_active_flag = False
                            return False
                else:
                    logger.warning("Unsuccessful attempt to change state Local Tiebreaker for %s Data Collector. "
                                   "(previous_response_successful = False)", dc_and_tb_state[current_ip_dc])
                    return False
        elif lv_object["secondaryStatus"] != "Up" and lv_object["primaryStatus"] == "Up":
            # this block execute when SC with secondary volume is Down. And in this case "Unprotected" state is normal situation!
            logger.warning("Secondary Volume of Live Volume not found")
            if lv_object["localTiebreaker"]:
                logger.info("Local Tiebreaker is already active for %s Data Collector", dc_and_tb_state[current_ip_dc])
                local_tiebreaker_use = dc_and_tb_state[current_ip_dc]
                tb_active_flag = True
            elif lv_object["localTiebreaker"] is False:
                logger.info("Changing to active state of Local Tiebreaker for %s Data Collector...",
                            dc_and_tb_state[current_ip_dc])
                execute(url_dict['Use_Local_TB'], 'post')  # change state of Local Tiebreaker
                if previous_response_successful:
                    if lv_object_after_change["localTiebreaker"]:
                        logger.info("Local Tiebreaker changed state to active for %s Data Collector",
                                    dc_and_tb_state[current_ip_dc])
                        local_tiebreaker_use = dc_and_tb_state[current_ip_dc]
                        tb_active_flag = True
                    else:
                        time.sleep(60)                      # time delay for repeat polling of Live Volume
                        logger.info("Repeated polling of state Live Volume for %s Data Collector", dc_and_tb_state[current_ip_dc])
                        execute(url_dict['LV_object'])      # get Live Volume object
                        if previous_response_successful:
                            if lv_object["localTiebreaker"]:
                                logger.info("Local Tiebreaker successfully changed state to active for %s Data Collector",
                                            dc_and_tb_state[current_ip_dc])
                                local_tiebreaker_use = dc_and_tb_state[current_ip_dc]
                                tb_active_flag = True
                            else:
                                logger.warning("Local Tiebreaker can not changed state to active for %s Data Collector",
                                               dc_and_tb_state[current_ip_dc])
                                local_tiebreaker_use = dc_and_tb_state["unknown_ip"]
                                tb_active_flag = False
                                return False
                        else:
                            logger.warning("State of Live Volume is not normal after change state of tiebreaker for %s "
                                           "Data Collector. (previous_response_successful = False) Go to new iteration...",
                                           dc_and_tb_state[current_ip_dc])
                            local_tiebreaker_use = dc_and_tb_state["unknown_ip"]
                            tb_active_flag = False
                            return False
                else:
                    logger.warning("Unsuccessful attempt to change state Local Tiebreaker for %s Data Collector. "
                                   "(previous_response_successful = False)", dc_and_tb_state[current_ip_dc])
                    return False
        elif lv_object["secondaryStatus"] == "Up" and lv_object["primaryStatus"] != "Up" \
                and lv_object["failoverState"] != "Unprotected" and lv_object["secondaryRole"] == "Activated":
            logger.warning("Live Volume successful recovery after crash, use_local_tb operation not needed while both of SHD will state Up")
            # for stop iteration while one of storage systems (SC4020) is not Up state
            local_tiebreaker_use = dc_and_tb_state["unknown_ip"]   # drop systems state for going to 5 attempt
            tb_active_flag = False
        else:
            logger.error("Unsuccessful attempt to change state Local Tiebreaker because both of volume is down")
            return False
    else:
        logger.warning("Skipped all call-function for change state %s Data Collector "
                       "(previous_response_successful = False). Go to new iteration...",
                        dc_and_tb_state[current_ip_dc])
        local_tiebreaker_use = dc_and_tb_state["unknown_ip"]
        tb_active_flag = False
        return False
    return True  # successful result of function


def previous_state_system_changed():
    global SHD1_state_up_previous
    global SHD2_state_up_previous
    global dc_primary_state_up_previous
    global dc_remote_state_up_previous
    global attempt_count
    global time_delay
    if SHD1_state_up_previous == SHD1_state_up and SHD2_state_up_previous == SHD2_state_up \
            and dc_primary_state_up_previous == dc_primary_state_up \
            and dc_remote_state_up_previous == dc_remote_state_up:
        return False
    else:
        if not (SHD1_state_up_previous is False and SHD2_state_up_previous is False
                and dc_primary_state_up_previous is False and dc_remote_state_up_previous is False):
            if (SHD1_state_up_previous is False and SHD1_state_up is True) \
                    or (SHD2_state_up_previous is False and SHD2_state_up is True):
                time.sleep(360)  # time delay 6 minutes after smth device Up for update information in Data Collector
            if (dc_primary_state_up_previous is False and dc_primary_state_up is True) \
                    or (dc_remote_state_up_previous is False and dc_remote_state_up is True):
                time.sleep(60)  # time delay 1 minute after smth device Up for update information in Data Collector
        attempt_count = 0
        time_delay = 10     # default value
        SHD1_state_up_previous = SHD1_state_up
        SHD2_state_up_previous = SHD2_state_up
        dc_primary_state_up_previous = dc_primary_state_up
        dc_remote_state_up_previous = dc_remote_state_up
        return True


def state_system_optimally():
    tiebreaker_state = "non optimal"
    volume_location = "non optimal"
    global attempt_count
    global time_delay
    if dc_primary_state_up is True and dc_remote_state_up is True:
        if (local_tiebreaker_use == "Primary" or local_tiebreaker_use == "Remote") and tb_active_flag is True:
            tiebreaker_state = "optimal"
    elif dc_primary_state_up is True and dc_remote_state_up is False:
        if local_tiebreaker_use == "Primary" and tb_active_flag is True:
            tiebreaker_state = "optimal"
    elif dc_primary_state_up is False and dc_remote_state_up is True:
        if local_tiebreaker_use == "Remote" and tb_active_flag is True:
            tiebreaker_state = "optimal"
    else:
        logger.warning("State of system is not optimal (DC reason)")
        return False
    if SHD1_state_up is True and SHD2_state_up is True:
        if primary_volume_location == "SHD1" or primary_volume_location == "SHD2":
            volume_location = "optimal"
    elif SHD1_state_up is True and SHD2_state_up is False:
        if primary_volume_location == "SHD1":
            volume_location = "optimal"
    elif SHD1_state_up is False and SHD2_state_up is True:
        if primary_volume_location == "SHD2":
            volume_location = "optimal"
    else:
        logger.warning("State of system is not optimal (SC reason)")
        return False
    if tiebreaker_state == "optimal" and volume_location == "optimal":
        return True
    else:
        attempt_count += 1
        if attempt_count >= 2:  # 3 attempt with 10 seconds time delay
            time_delay = 90     # 3 attempt with 90 seconds time delay
        if attempt_count >= 5:  # drop count in previous_state_system_changed function
            time_delay = 10     # default value
        if attempt_count >= 6:  # drop count in previous_state_system_changed function
            return True
        else:
            return False


def current_dc_determine():
    global current_ip_dc
    if dc_primary_state_up is True and dc_remote_state_up is True:
        if primary_volume_location == "SHD1":
            current_ip_dc = primary_ip
        elif primary_volume_location == "SHD2":
            current_ip_dc = remote_ip
        else:
            # this block can be run when script start only, when primary_volume_location = "Unknown"
            if dc_primary_state_up is True:
                current_ip_dc = primary_ip
            elif dc_remote_state_up_previous is True:
                current_ip_dc = remote_ip
    elif dc_primary_state_up is True and dc_remote_state_up is False:
        current_ip_dc = primary_ip
    elif dc_primary_state_up is False and dc_remote_state_up is True:
        current_ip_dc = remote_ip


def primary_volume_search():
    global primary_volume_location
    try:
        if previous_response_successful is False:
            logger.error("Previous operation finished unsuccessful. Go to new iteration")
            return False
        if lv_object['primaryStatus'] != "Up" and lv_object['failoverState'] == "Unprotected":
            # cut down situation when primary volume is down but resolve situation when LV restore after crash
            logger.error("Primary volume in configuration of Live Volume can't be found. Go to new  iteration")
            return False  # for new iteration if primary volume is down
        if SHD1_state_up is True and SHD2_state_up is True:
            if sc_list['SHD1']['connected'] is True and sc_list['SHD2']['connected'] is True \
                    and sc_list['SHD1']['status'] != "Down" and sc_list['SHD2']['status'] != "Down":
                if lv_object['instanceName'] == "Live Volume of " + name_volume['first'] \
                        and volume_and_SC_roles['primaryStorageCenter'] == "SHD1" \
                        and volume_and_SC_roles['primaryVolume'] == name_volume['first'] \
                        and volume_and_SC_roles['secondaryStorageCenter'] == "SHD2" \
                        and volume_and_SC_roles['secondaryVolume'] == name_volume['second']:
                    primary_volume_location = "SHD1"
                elif lv_object['instanceName'] == "Live Volume of " + name_volume['second'] \
                        and volume_and_SC_roles['primaryStorageCenter'] == "SHD2" \
                        and volume_and_SC_roles['primaryVolume'] == name_volume['second'] \
                        and volume_and_SC_roles['secondaryStorageCenter'] == "SHD1" \
                        and volume_and_SC_roles['secondaryVolume'] == name_volume['first']:
                    primary_volume_location = "SHD2"
                else:
                    primary_volume_location = "Unknown"
            elif sc_list['SHD1']['connected'] is True and sc_list['SHD2']['connected'] is False \
                    and sc_list['SHD1']['status'] != "Down" and sc_list['SHD2']['status'] == "Down":
                primary_volume_location = "SHD1"
            elif sc_list['SHD1']['connected'] is False and sc_list['SHD2']['connected'] is True \
                    and sc_list['SHD1']['status'] == "Down" and sc_list['SHD2']['status'] != "Down":
                primary_volume_location = "SHD2"
            else:
                primary_volume_location = "Unknown"
        elif SHD1_state_up is True and SHD2_state_up is False:
            if lv_object['primaryStatus'] == "Up" \
                    and volume_and_SC_roles['primaryStorageCenter'] == "SHD1" \
                    and volume_and_SC_roles['secondaryStorageCenter'] == "SHD2":
                primary_volume_location = "SHD1"
            else:
                primary_volume_location = "Unknown"
        elif SHD1_state_up is False and SHD2_state_up is True:
            if lv_object['primaryStatus'] == "Up" \
                    and volume_and_SC_roles['primaryStorageCenter'] == "SHD2" \
                    and volume_and_SC_roles['secondaryStorageCenter'] == "SHD1":
                primary_volume_location = "SHD2"
            else:
                primary_volume_location = "Unknown"
    except KeyError:
        logger.error("Problem with mismatch of parameter list between received from DC and declared in API documentation")
        primary_volume_location = "Unknown"
    logger.info("Primary volume location: %s", primary_volume_location)
    return True

def swap_volume_role():
    logger.info("Call of function 'swap_volume_role'")
    if SHD1_state_up is True and SHD2_state_up is True and (dc_primary_state_up is False or dc_remote_state_up is False):
        if dc_primary_state_up is True and dc_remote_state_up is False and primary_volume_location == "SHD1":
            logger.info("Swapping roles of volume not need. The state of system is optimal")
            return
        elif dc_primary_state_up is False and dc_remote_state_up is True and primary_volume_location == "SHD2":
            logger.info("Swapping roles of volume not need. The state of system is optimal")
            return
        elif primary_volume_location == "Unknown":
            logger.warning("Swapping role can't apply because undefined location of primary volume")
            return
        logger.info("Need to make a swap roles. Checking state to determine if it is possible...")
        try:
            logger.info("Call of function for determine state of UPS in 320 and 420 rooms")
            if check_state_ups(ups_ip, oid_list, primary_volume_location) is False:
                logger.info("UPS PARAMETERS: operation 'swapping roles of volume' is not apply. Because state of power "
                            "system in destination room is not good")
                return
            else:
                logger.info("UPS PARAMETERS: state of power system in destination room is good")
        except Exception:
            logger.error("Are common problem in process of function for determine state of power system. "
                         "State of power system is unknown. Swap role does not apply")
            return
        previous_location_primary_volume = primary_volume_location
        try:
            if lv_object['status'] == "Up" and lv_object['replicationState'] == "Up" \
                    and lv_object["localTiebreaker"] is True and lv_object["failoverState"] == "Protected" \
                    and lv_object['primaryStatus'] == "Up" and lv_object['secondaryStatus'] == "Up" \
                    and lv_object['primaryRole'] == "Primary" and lv_object['secondaryRole'] == "Secondary" \
                    and lv_object['primarySwapRoleState'] == "NotSwapping" \
                    and lv_object['secondarySwapRoleState'] == "NotSwapping" \
                    and replic_object['state'] == "Up" and replic_object['percentComplete'] == 100 \
                    and replic_object['synced'] is True and lv_object['swappingRoles'] is False:
                execute(url_dict['swapRoles'], 'post')
                time.sleep(180)  # timeout after swap 3 minutes
                if previous_response_successful is True:
                    logger.info("URL for swap apply successful. Checking state after swap...")
                    clean_urls()
                    if state_storage_center_determine() is False:
                        logger.error("Problem with determine state of Live Volume after apply swap volume operation")
                        return
                    if primary_volume_search():
                        logger.info("Location of Primary volume before swap: %s", previous_location_primary_volume)
                        logger.info("Location of Primary volume after swap: %s", primary_volume_location)
                        if previous_location_primary_volume != primary_volume_location:
                            logger.warning("Roles of Volume changed successful")
                        else:
                            logger.warning("Roles of Volume not changed")
                else:
                    logger.warning("Roles of Volume not changed")
            else:
                logger.warning("It is impossible to switch roles. Current configuration of storage centers "
                               "does not support this operation")
        except KeyError:
            logger.warning("Problem with key-value in process of swapping role")
    else:
        if SHD1_state_up is False or SHD2_state_up is False:
            logger.info("For current state of system swapping role can't apply")
        else:
            logger.info("For current state of system swapping role is not needed")


def port_rebalanced():
    logger.info("Call function for rebalance ports")
    execute(url_dict['SC_list_object'], 'post')  # determine the need for port balancing and prepare urls
    if SHD1_port_rebalance_needed is False and SHD2_port_rebalance_needed is False:
        logger.info("Rebalancing ports is not needed")
        return
    logger.info("Ports rebalance needed for SHD1: %s. Ports rebalance needed for SHD2: %s",
                SHD1_port_rebalance_needed, SHD2_port_rebalance_needed)
    SHD1_port_rebalance_needed_previous = SHD1_port_rebalance_needed
    SHD2_port_rebalance_needed_previous = SHD2_port_rebalance_needed
    try:
        if previous_response_successful:
            if SHD1_state_up is True and SHD2_state_up is True:
                if lv_object['replicationState'] == "Up" and lv_object['primaryStatus'] == "Up" \
                        and lv_object['secondaryStatus'] == "Up" and lv_object['primaryRole'] == "Primary" \
                        and lv_object['secondaryRole'] == "Secondary" \
                        and lv_object['primarySwapRoleState'] == "NotSwapping" \
                        and lv_object['secondarySwapRoleState'] == "NotSwapping" \
                        and replic_object['state'] == "Up" and replic_object['percentComplete'] == 100 \
                        and replic_object['synced'] is True and lv_object['swappingRoles'] is False:
                    if SHD1_port_rebalance_needed:
                        execute(url_dict['Port_rebalance_SHD1'], 'post')
                        time.sleep(10)
                    if SHD2_port_rebalance_needed:
                        execute(url_dict['Port_rebalance_SHD2'], 'post')
                        time.sleep(10)
                else:
                    logger.error("Can't apply port balancing operation due to state of replication")
            elif SHD1_state_up is True and SHD2_state_up is False:
                execute(url_dict['Port_rebalance_SHD1'], 'post')
                time.sleep(10)
            elif SHD1_state_up is False and SHD2_state_up is True:
                execute(url_dict['Port_rebalance_SHD2'], 'post')
                time.sleep(10)
            else:
                return
        else:
            logger.error("Operation of rebalance ports can't apply, because previous operation is not complete successfully")
            return
    except KeyError:
        logger.warning("Problem with key-value in process of rebalancing ports")
    execute(url_dict['SC_list_object'], 'post')  # check the need for port balancing
    if previous_response_successful:
        if SHD1_state_up is True and SHD2_state_up is True:
            if SHD1_port_rebalance_needed is False and SHD2_port_rebalance_needed is False and \
                    SHD1_port_rebalance_needed_previous is True and SHD2_port_rebalance_needed_previous is True:
                logger.info("Ports rebalanced successful for both controllers")
            elif SHD1_port_rebalance_needed is False and SHD2_port_rebalance_needed is False and \
                    SHD1_port_rebalance_needed_previous is True and SHD2_port_rebalance_needed_previous is False:
                logger.info("Ports rebalanced successful for SHD1")
            elif SHD1_port_rebalance_needed is True and SHD2_port_rebalance_needed is False and \
                    SHD1_port_rebalance_needed_previous is True and SHD2_port_rebalance_needed_previous is False:
                logger.info("Ports rebalanced unsuccessful for SHD1")
            elif SHD1_port_rebalance_needed is False and SHD2_port_rebalance_needed is False and \
                    SHD1_port_rebalance_needed_previous is False and SHD2_port_rebalance_needed_previous is True:
                logger.info("Ports rebalanced successful for SHD2")
            elif SHD1_port_rebalance_needed is False and SHD2_port_rebalance_needed is True and \
                    SHD1_port_rebalance_needed_previous is False and SHD2_port_rebalance_needed_previous is True:
                logger.info("Ports rebalanced unsuccessful for SHD2")
            elif SHD1_port_rebalance_needed is True and SHD2_port_rebalance_needed is True and \
                    SHD1_port_rebalance_needed_previous is True and SHD2_port_rebalance_needed_previous is True:
                logger.info("Ports rebalanced unsuccessful for both SHD")
            elif SHD1_port_rebalance_needed is False and SHD2_port_rebalance_needed is True and \
                    SHD1_port_rebalance_needed_previous is True and SHD2_port_rebalance_needed_previous is True:
                logger.info("Ports rebalanced successful for SHD1 and unsuccessful for SHD2")
            elif SHD1_port_rebalance_needed is True and SHD2_port_rebalance_needed is False and \
                    SHD1_port_rebalance_needed_previous is True and SHD2_port_rebalance_needed_previous is True:
                logger.info("Ports rebalanced successful for SHD2 and unsuccessful for SHD1")
            else:
                logger.warning("Ports rebalanced unsuccessful on both or ones controllers. "
                               "Ports rebalance needed for SHD1: %s. Ports rebalance needed for SHD2: %s",
                               SHD1_port_rebalance_needed, SHD2_port_rebalance_needed)
        elif SHD1_state_up is True and SHD2_state_up is False:
            if SHD1_port_rebalance_needed is False and SHD1_port_rebalance_needed_previous is True:
                logger.info("Ports rebalanced successful for SHD1")
            else:
                logger.info("Ports rebalanced unsuccessful for SHD1")
        elif SHD1_state_up is False and SHD2_state_up is True:
            if SHD2_port_rebalance_needed is False and SHD2_port_rebalance_needed_previous is True:
                logger.info("Ports rebalanced successful for SHD2")
            else:
                logger.info("Ports rebalanced unsuccessful for SHD2")
        else:
            logger.warning("Ports rebalanced unsuccessful on both or ones controllers. "
                           "Ports rebalance needed for SHD1: %s. Ports rebalance needed for SHD2: %s",
                           SHD1_port_rebalance_needed, SHD2_port_rebalance_needed)
    else:
        logger.error("Request for check state after rebalance ports is not successful")


def disconnect_from_dc(ip):
    execute(url_dict['logout'], ip_dc=ip)


if __name__ == '__main__':
    logger.info("START")
    while True:
        time.sleep(time_delay)              # Time delay 10 seconds: cycle of polling
        if check_state_disks_array():
            if check_state_data_collector():
                if previous_state_system_changed() is True or state_system_optimally() is False or dc_reboot_detected is True:
                    dc_reboot_detected = False
                    time.sleep(10)             # Time delay for update state of information in Data Collector
                    current_dc_determine()    # Determined through what data collector connection will be done
                    if connect_to_data_collector(current_ip_dc):
                        if state_storage_center_determine():
                            if primary_volume_search():  # determine what volume is primary
                                previous_ip_dc = current_ip_dc
                                current_dc_determine()   # taking into account what volume has Primary role (when script starting)
                                if previous_ip_dc != current_ip_dc:
                                    disconnect_from_dc(previous_ip_dc)
                                    clean_urls()
                                    logger.info("Repeat of connection taking into account where locate Primary Volume")
                                    if connect_to_data_collector(current_ip_dc) is False:
                                        continue
                                    elif state_storage_center_determine() is False:
                                        logger.warning("There is problem with check of state storage center")
                                        disconnect_from_dc(current_ip_dc)
                                        continue
                                tb_switch()    # now current_ip_dc has actual optimal value
                                port_rebalanced()
                                swap_volume_role()
                        else:
                            logger.warning("There is problem with check of state storage center")
                        disconnect_from_dc(current_ip_dc)
        else:
            logger.warning("SHD1 (disks array SC4020 in 420 room) and SHD2 (disks array SC4020 in 320 room) does not respond to ping")
        clean_urls()
        print("NEXT ITERATION")
        logger.info("NEXT ITERATION")
