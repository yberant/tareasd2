package data_name

type Server struct{

}

func(server *Server) RequestOrder(stream DataName_RequestOrderServer) error{
	return nil
}

func(Server *Server) InformOrder(stream DataName_InformOrderServer) error{
	return nil
}