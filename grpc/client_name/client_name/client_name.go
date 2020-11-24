package client_name


type Server struct{

}

//una vez tenga almacenado ordenes en el log, namenode envía al cliente info del orden
//que yo sepa, esta parte en sí NO requiere usar el algoritmo de ricart/agrawala
func (server *Server) ChunksOrder(req *OrderReq, stream ClientName_ChunksOrderServer) error{
	
	/*
	fileName:=req.GetFilename()
	orders:=[]OrderRes{}
	... buscar el orden de chunks del archivo filename en el log y almacenarlo en orders...
	recordar que OrderRes tiene dos variables ChunkId (int64) y NodeId(int64)


	for _,order:=range orders{
		stream.Send(&order)
	}

	*/
	
	return nil
}

