# rtsp_ws
Websocket server that serves metadata in V2X expected format

Update ENV with rtsp link in docker file as needed


# Docker commands to setup container
docker build -t websocket-server .
docker run -p 8080:80 websocket-server