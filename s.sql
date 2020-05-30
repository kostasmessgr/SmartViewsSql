select last_value(pk) from thesis.dims;
use thesis;
CREATE TABLE IF NOT EXISTS dims( 
  `A` int(10) ,
  `B` int(10) ,
  `C` int(10) ,
  `D` int(10) ,
  `E` int(10) ,
  `pk` int(10),
  `id` int(10) auto_increment,
  primary key(id)
);
CREATE TABLE IF NOT EXISTS gbStruct( 
  `id` int(10) auto_increment,
  `hash` varchar(200) ,
  `latestFact` int(10) ,
  `size` int(20) ,
  `colSize` int(10) ,
  `columns` varchar(40) ,
  `aggrFunc` varchar(25),
  `timestamp` int(20),
  primary key(id)
);
select * from dims order by id desc limit 1;
use thesis;
CREATE TABLE IF NOT EXISTS ids( 
  `pk` int(10) not null,
  primary key(pk)
);
select count(*) from thesis.dims;
select * from thesis.ids;
drop table thesis.gbStruct;
drop table thesis.dims;
delete from thesis.dims;
delete from thesis.ids;
delete from thesis.gbstruct;
select * from thesis.gbStruct;
select id from dims order by id desc limit 1;
create table idTable( 
'id' int(10) not null,
primary key(id)
);
select * from thesis.dims;
delete from gbStruct where id = 1838;
insert into tempTbl (A, B, C, D, E, pk) values (1620, 1957, 1377, 1207, null, 0), (1021, 1056, 1183, 1993, null, 1), (1198, 1405, 1695, 1825, null, 2), (1531, 1641, 1249, 1841, null, 3), (1033, 1664, 1854, 1527, null, 4), (1061, 1125, 1778, 1873, null, 5), (1323, 1214, 1690, 1067, null, 6), (1034, 1024, 1876, 1225, null, 7), (1976, 1844, 1732, 1436, null, 8), (1393, 1345, 1837, 1016, null, 9), (1863, 1964, 1758, 1840, null, 10), (1155, 1518, 1747, 1570, null, 11), (1151, 1242, 1522, 1190, null, 12), (1894, 1873, 1835, 1497, null, 13), (1466, 1341, 1955, 1683, null, 14), (1934, 1573, 1703, 1113, null, 15), (1265, 1941, 1837, 1565, null, 16), (1079, 1613, 1696, 1535, null, 17), (1127, 1440, 1706, 1476, null, 18), (1899, 1415, 1252, 1744, null, 19), (1267, 1351, 1108, 1089, null, 20), (1560, 1167, 1805, 1612, null, 21), (1665, 1664, 1484, 1498, null, 22), (1528, 1953, 1841, 1845, null, 23), (1047, 1986, 1773, 1705, null, 24), (1084, 1173, 1723, 1323, null, 25), (1619, 1574, 1660, 1183, null, 26), (1921, 1210, 1597, 1379, null, 27), (1738, 1155, 1880, 1305, null, 28), (1927, 1955, 1124, 1544, null, 29), (1394, 1596, 1162, 1403, null, 30), (1331, 1936, 1751, 1651, null, 31), (1235, 1355, 1938, 1794, null, 32), (1655, 1790, 1905, 1310, null, 33), (1357, 1287, 1959, 1754, null, 34), (1847, 1097, 1296, 1905, null, 35), (1264, 1962, 1146, 1041, null, 36), (1209, 1258, 1763, 1009, null, 37), (1323, 1747, 1922, 1531, null, 38), (1533, 1626, 1767, 1157, null, 39), (1980, 1103, 1129, 1976, null, 40), (1239, 1212, 1577, 1758, null, 41), (1563, 1428, 1682, 1023, null, 42), (1714, 1525, 1001, 1397, null, 43), (1851, 1138, 1048, 1527, null, 44), (1682, 1145, 1640, 1250, null, 45), (1350, 1425, 1372, 1751, null, 46), (1325, 1349, 1150, 1604, null, 47), (1075, 1943, 1358, 1080, null, 48), (1159, 1669, 1477, 1058, null, 49), (1141, 1188, 1894, 1779, null, 50), (1538, 1902, 1457, 1762, null, 51), (1967, 1477, 1641, 1421, null, 52), (1420, 1296, 1889, 1191, null, 53), (1455, 1955, 1435, 1786, null, 54), (1244, 1328, 1953, 1464, null, 55), (1868, 1113, 1695, 1500, null, 56), (1513, 1119, 1778, 1629, null, 57), (1723, 1181, 1140, 1660, null, 58), (1001, 1646, 1978, 1385, null, 59), (1226, 1667, 1138, 1265, null, 60), (1146, 1098, 1120, 1388, null, 61), (1144, 1319, 1522, 1105, null, 62), (1975, 1844, 1827, 1254, null, 63), (1908, 1591, 1483, 1644, null, 64), (1171, 1623, 1852, 1879, null, 65), (1129, 1040, 1144, 1537, null, 66), (1024, 1666, 1226, 1936, null, 67), (1505, 1885, 1157, 1726, null, 68), (1122, 1189, 1406, 1142, null, 69), (1207, 1624, 1352, 1331, null, 70), (1300, 1803, 1132, 1810, null, 71), (1005, 1192, 1117, 1643, null, 72), (1817, 1345, 1366, 1374, null, 73), (1300, 1435, 1835, 1907, null, 74), (1277, 1275, 1818, 1761, null, 75), (1728, 1606, 1627, 1457, null, 76), (1681, 1130, 1008, 1562, null, 77), (1627, 1602, 1311, 1029, null, 78), (1216, 1632, 1370, 1327, null, 79), (1961, 1193, 1794, 1435, null, 80), (1802, 1824, 1775, 1154, null, 81), (1476, 1357, 1554, 1732, null, 82), (1986, 1632, 1550, 1490, null, 83), (1850, 1108, 1992, 1214, null, 84), (1360, 1465, 1689, 1254, null, 85), (1004, 1910, 1714, 1774, null, 86), (1720, 1619, 1324, 1335, null, 87), (1193, 1119, 1901, 1861, null, 88), (1607, 1682, 1684, 1968, null, 89), (1263, 1392, 1861, 1651, null, 90), (1256, 1139, 1847, 1132, null, 91), (1742, 1423, 1537, 1779, null, 92), (1518, 1078, 1649, 1309, null, 93), (1701, 1644, 1634, 1655, null, 94), (1209, 1893, 1968, 1624, null, 95), (1932, 1076, 1056, 1994, null, 96), (1536, 1205, 1909, 1185, null, 97), (1625, 1947, 1068, 1469, null, 98), (1065, 1848, 1083, 1444, null, 99), (1324, 1425, 1714, 1921, null, 100), (1187, 1174, 1886, 1726, null, 101), (1500, 1968, 1433, 1793, null, 102), (1720, 1869, 1107, 1185, null, 103), (1303, 1806, 1996, 1833, null, 104), (1411, 1545, 1473, 1675, null, 105), (1669, 1088, 1700, 1662, null, 106), (1514, 1550, 1774, 1445, null, 107), (1207, 1478, 1119, 1424, null, 108), (1954, 1222, 1241, 1505, null, 109), (1959, 1858, 1417, 1677, null, 110), (1076, 1692, 1816, 1840, null, 111), (1544, 1325, 1643, 1150, null, 112), (1441, 1493, 1004, 1966, null, 113), (1133, 1376, 1875, 1874, null, 114), (1254, 1708, 1439, 1841, null, 115), (1048, 1099, 1303, 1964, null, 116), (1915, 1746, 1217, 1210, null, 117), (1638, 1116, 1484, 1421, null, 118), (1934, 1890, 1234, 1414, null, 119), (1532, 1857, 1147, 1935, null, 120), (1930, 1700, 1130, 1175, null, 121), (1764, 1246, 1120, 1669, null, 122), (1348, 1171, 1816, 1920, null, 123), (1426, 1435, 1659, 1931, null, 124), (1601, 1149, 1887, 1071, null, 125), (1232, 1255, 1091, 1286, null, 126), (1621, 1457, 1389, 1709, null, 127), (1715, 1271, 1942, 1358, null, 128), (1505, 1152, 1402, 1994, null, 129), (1380, 1560, 1361, 1454, null, 130), (1213, 1920, 1535, 1269, null, 131), (1306, 1010, 1157, 1298, null, 132), (1265, 1032, 1290, 1550, null, 133), (1939, 1745, 1626, 1461, null, 134), (1612, 1075, 1329, 1201, null, 135), (1863, 1567, 1796, 1202, null, 136), (1615, 1571, 1674, 1638, null, 137), (1183, 1892, 1952, 1538, null, 138), (1573, 1813, 1164, 1473, null, 139), (1651, 1928, 1557, 1550, null, 140), (1104, 1221, 1587, 1121, null, 141), (1137, 1157, 1868, 1779, null, 142), (1574, 1600, 1049, 1635, null, 143), (1390, 1329, 1987, 1304, null, 144), (1022, 1963, 1239, 1397, null, 145), (1541, 1370, 1388, 1332, null, 146), (1955, 1533, 1311, 1235, null, 147), (1155, 1689, 1567, 1030, null, 148), (1799, 1617, 1320, 1342, null, 149), (1680, 1666, 1458, 1065, null, 150), (1259, 1632, 1431, 1235, null, 151), (1738, 1859, 1180, 1505, null, 152), (1992, 1158, 1136, 1344, null, 153), (1550, 1021, 1028, 1396, null, 154), (1121, 1696, 1056, 1708, null, 155), (1595, 1640, 1476, 1087, null, 156), (1323, 1536, 1064, 1675, null, 157), (1740, 1312, 1320, 1480, null, 158), (1428, 1967, 1658, 1558, null, 159), (1404, 1056, 1024, 1675, null, 160), (1638, 1558, 1470, 1029, null, 161), (1487, 1772, 1861, 1174, null, 162), (1286, 1361, 1374, 1551, null, 163), (1487, 1698, 1657, 1625, null, 164), (1066, 1495, 1588, 1032, null, 165), (1484, 1077, 1357, 1237, null, 166), (1607, 1937, 1822, 1254, null, 167), (1518, 1231, 1831, 1483, null, 168), (1923, 1491, 1616, 1402, null, 169), (1660, 1736, 1794, 1327, null, 170), (1400, 1084, 1415, 1199, null, 171), (1550, 1491, 1406, 1966, null, 172), (1587, 1460, 1981, 1501, null, 173), (1320, 1026, 1670, 1598, null, 174), (1351, 1763, 1737, 1418, null, 175), (1819, 1337, 1840, 1916, null, 176), (1176, 1142, 1174, 1942, null, 177), (1904, 1032, 1014, 1612, null, 178), (1217, 1024, 1893, 1741, null, 179), (1111, 1477, 1270, 1632, null, 180), (1653, 1036, 1807, 1976, null, 181), (1175, 1744, 1599, 1534, null, 182), (1267, 1135, 1443, 1585, null, 183), (1853, 1639, 1184, 1061, null, 184), (1294, 1101, 1133, 1229, null, 185), (1729, 1441, 1507, 1039, null, 186), (1067, 1662, 1996, 1673, null, 187), (1356, 1268, 1353, 1832, null, 188), (1866, 1424, 1318, 1246, null, 189), (1035, 1035, 1678, 1449, null, 190), (1074, 1314, 1558, 1819, null, 191), (1205, 1580, 1019, 1056, null, 192), (1917, 1522, 1563, 1163, null, 193), (1579, 1842, 1502, 1510, null, 194), (1111, 1432, 1446, 1347, null, 195), (1510, 1405, 1733, 1475, null, 196), (1123, 1118, 1347, 1293, null, 197), (1821, 1142, 1908, 1634, null, 198), (1217, 1817, 1037, 1982, null, 199), (1076, 1256, 1681, 1179, null, 1000), (1957, 1414, 1063, 1424, null, 1001), (1394, 1602, 1911, 1789, null, 1002), (1495, 1970, 1144, 1587, null, 1003), (1485, 1137, 1159, 1802, null, 1004), (1726, 1696, 1941, 1131, null, 1005), (1838, 1140, 1508, 1095, null, 1006), (1884, 1024, 1590, 1751, null, 1007), (1032, 1096, 1739, 1855, null, 1008), (1339, 1184, 1116, 1582, null, 1009), (1261, 1621, 1689, 1433, null, 1010), (1564, 1345, 1823, 1719, null, 1011), (1024, 1569, 1347, 1461, null, 1012), (1152, 1655, 1558, 1377, null, 1013), (1569, 1124, 1450, 1352, null, 1014), (1869, 1380, 1932, 1040, null, 1015), (1938, 1113, 1636, 1552, null, 1016), (1918, 1225, 1036, 1915, null, 1017), (1101, 1759, 1435, 1564, null, 1018), (1868, 1154, 1785, 1498, null, 1019), (1578, 1134, 1405, 1656, null, 1020), (1189, 1302, 1651, 1866, null, 1021), (1622, 1569, 1866, 1665, null, 1022), (1702, 1521, 1573, 1061, null, 1023), (1183, 1245, 1343, 1232, null, 1024), (1863, 1298, 1795, 1403, null, 1025), (1607, 1016, 1065, 1724, null, 1026), (1359, 1398, 1494, 1564, null, 1027), (1679, 1009, 1479, 1859, null, 1028), (1947, 1164, 1685, 1923, null, 1029), (1028, 1039, 1394, 1918, null, 1030), (1865, 1747, 1800, 1461, null, 1031), (1887, 1448, 1656, 1315, null, 1032), (1580, 1979, 1849, 1687, null, 1033), (1636, 1278, 1586, 1573, null, 1034), (1061, 1735, 1426, 1591, null, 1035), (1720, 1330, 1709, 1465, null, 1036), (1287, 1420, 1425, 1792, null, 1037), (1968, 1027, 1039, 1989, null, 1038), (1848, 1895, 1182, 1881, null, 1039), (1461, 1297, 1960, 1378, null, 1040), (1446, 1852, 1726, 1518, null, 1041), (1887, 1574, 1660, 1880, null, 1042), (1993, 1863, 1956, 1485, null, 1043), (1112, 1939, 1928, 1179, null, 1044), (1834, 1624, 1161, 1177, null, 1045), (1099, 1541, 1220, 1425, null, 1046), (1254, 1240, 1266, 1097, null, 1047), (1794, 1705, 1270, 1982, null, 1048), (1384, 1898, 1222, 1873, null, 1049), (1800, 1158, 1576, 1270, null, 1050), (1141, 1818, 1771, 1630, null, 1051), (1413, 1359, 1160, 1387, null, 1052), (1395, 1304, 1819, 1228, null, 1053), (1881, 1551, 1384, 1467, null, 1054), (1251, 1948, 1466, 1279, null, 1055), (1674, 1135, 1940, 1362, null, 1056), (1530, 1785, 1693, 1605, null, 1057), (1966, 1282, 1666, 1498, null, 1058), (1206, 1078, 1272, 1225, null, 1059), (1696, 1609, 1061, 1641, null, 1060), (1568, 1221, 1668, 1922, null, 1061), (1086, 1833, 1437, 1884, null, 1062), (1175, 1194, 1046, 1629, null, 1063), (1461, 1093, 1348, 1723, null, 1064), (1445, 1412, 1761, 1026, null, 1065), (1627, 1952, 1799, 1437, null, 1066), (1930, 1732, 1366, 1568, null, 1067), (1150, 1669, 1419, 1674, null, 1068), (1810, 1927, 1631, 1590, null, 1069), (1634, 1599, 1037, 1666, null, 1070), (1227, 1291, 1036, 1032, null, 1071), (1826, 1261, 1354, 1818, null, 1072), (1700, 1695, 1302, 1896, null, 1073), (1519, 1851, 1908, 1719, null, 1074), (1036, 1939, 1372, 1922, null, 1075), (1515, 1438, 1030, 1584, null, 1076), (1650, 1913, 1437, 1712, null, 1077), (1557, 1562, 1049, 1927, null, 1078), (1510, 1828, 1109, 1080, null, 1079), (1769, 1279, 1545, 1300, null, 1080), (1780, 1946, 1042, 1639, null, 1081), (1364, 1211, 1459, 1810, null, 1082), (1405, 1298, 1332, 1637, null, 1083), (1042, 1948, 1174, 1620, null, 1084), (1680, 1036, 2000, 1148, null, 1085), (1780, 1899, 1349, 1242, null, 1086), (1975, 1246, 1234, 1551, null, 1087), (1753, 1601, 1826, 1459, null, 1088), (1782, 1088, 1935, 1454, null, 1089), (1845, 1001, 1726, 1604, null, 1090), (1160, 1092, 1191, 1174, null, 1091), (1514, 1877, 1963, 1594, null, 1092), (1311, 1364, 1330, 1416, null, 1093), (1280, 1592, 1938, 1097, null, 1094), (1258, 1713, 1062, 1197, null, 1095), (1455, 1595, 1232, 1187, null, 1096), (1559, 1435, 1753, 1926, null, 1097), (1177, 1051, 1016, 1818, null, 1098), (1536, 1156, 1248, 1805, null, 1099);
show variables like 'max_allowed_packet'
set global max_allowed_packet=5242880000
SELECT last_insert_id(pk) from thesis.ids;
show variables like 'wait_timeout';