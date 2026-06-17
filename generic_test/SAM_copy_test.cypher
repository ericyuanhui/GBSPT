COPY `User` FROM "static/User_0_0.csv";
COPY `Session` FROM "static/Session_0_0.csv";
COPY `NASDevice` FROM "static/NASDevice_0_0.csv";
COPY `Domain` FROM "static/Domain_0_0.csv";

COPY `User_owns_Session` FROM "dynamic/User_owns_Session_0_0.csv";
COPY `Session_via_NAS` FROM "dynamic/Session_via_NAS_0_0.csv";
COPY `User_belongsTo_Domain` FROM "dynamic/User_belongsTo_Domain_0_0.csv";
