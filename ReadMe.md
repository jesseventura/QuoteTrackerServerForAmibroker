# a QuoteTracker server(local streaming csv data)
# aligned with QT the QuoteTracker Amibroker Plugin (client)

### On Client(Amibroker.exe) Side:
**1.** install the QT plugin

**2.** set server ip/port in plugin configure

### On Server Side:
**1.** flask --debug --app qtserver run --debugger --reload --host="0.0.0.0" --port=PORT

**2.** zmq/upstream data -> local streaming csv data in the location where the "datdir" variable is set

