package client_data

import(
	"math/rand"
	"fmt"
	"context"
	"time"
	data_data "../../data_data/data_data"
)

//esta versión solo se utilizaría en la distribuida
func (server *Server) SendChunksToOtherDataNodes(chunks []*Chunk, fileName string) error{
	fmt.Println("starting")
	rand.Seed(time.Now().UnixNano())
	fmt.Println("getting the id of other datanode A")
	IDA:=server.FriendIdA
	//idreq:=data_data.IdReq{Data: "..."}
	/*IDA,err:=server.OtherDataNodeA.GetId(context.Background(),&idreq)
	fmt.Println("received response for the id")
	if err!=nil{
		fmt.Printf("error: %v\n",err)
		return err
	}*/
	fmt.Println("received node id")
	fmt.Printf("that id is: \n",IDA)
	/*IDB,err:=server.OtherDataNodeB.GetId(context.Background(),&data_data.IdReq{})
	if err!=nil{
		return err
	}*/


	defineorder:
	fmt.Println("defining order")
	numberOfChunks:=len(chunks)
	fmt.Println("number of chunks: %d",numberOfChunks)

	nodeidsorders:=[]int64{}
	//para asegurar de que cada datanode tenga como minimo un chunk, los primeros 3 chunks iran para cada nodo
	nodeidsorders=append(nodeidsorders,1)
	nodeidsorders=append(nodeidsorders,2)
	nodeidsorders=append(nodeidsorders,3)
	fmt.Print("actual order (first 3): ")
	fmt.Println(nodeidsorders)
	fmt.Println("shuffling...")
	rand.Shuffle(len(nodeidsorders),func(i, j int) { nodeidsorders[i], nodeidsorders[j] = nodeidsorders[j], nodeidsorders[i] })
	fmt.Println("shuffled")
	if numberOfChunks>=3{
		chunksleft:=numberOfChunks-3
		for chunksleft>0{
			nodeidsorders=append(nodeidsorders,int64(rand.Intn(3)+1))
			chunksleft-=1
		}
	} else{
		nodeidsorders=nodeidsorders[numberOfChunks:]
	}

	fmt.Print("final orders: ")
	fmt.Println(nodeidsorders)
	fmt.Println("adding the filename: "+fileName+" to the order requests")
	directions:=[]*data_data.OrderReq{&data_data.OrderReq{
		Req: &data_data.OrderReq_FileName{
			FileName: fileName,
		},
	}}
	fmt.Println("adding the orderrequests")
	for i,order:=range nodeidsorders{
		directions=append(directions,&data_data.OrderReq{
			Req: &data_data.OrderReq_OrderData{
				OrderData: &data_data.OrderData{
					NodeId: order,
					ChunkId: int64(i),		
				},
			},
		})
	}
	//aca debería haver un if que pregunte si es distribuido o centralizado. En este caso es distribuido
	err,aprobation:=server.DistributedRequest(directions)
	fmt.Print("aproved?: ")
	fmt.Println(aprobation)
	if err!=nil{
		return err
	}

	if aprobation==false{//si se rechazó la propuesta, se vuelve a armar otra
		goto defineorder
	}
	
	
	fmt.Println("saving the filename to transfer")
	//una vez se aprobaron, creo los arreglos en los que se enviarán la info. Parto con los nombres de archivos
	fileNameData:=&data_data.TransferReq{
		Req: &data_data.TransferReq_FileName{
			FileName: fileName,
	}}
	chunks1:=[]*data_data.TransferReq{fileNameData}
	chunks2:=[]*data_data.TransferReq{fileNameData}
	chunks3:=[]*data_data.TransferReq{fileNameData}
	fmt.Println("saving the chunks to transfer")
	//distribuyo que chunks irán para cada datanode
	for i,node:=range nodeidsorders{
		newchunk:=&data_data.TransferReq{
			Req:&data_data.TransferReq_DataChunk{
				DataChunk: &data_data.Chunk{
					Content: chunks[i].Content,
					ChunkId: int64(i),
				},
			},
		}
		switch node{
		case 1:
			chunks1=append(chunks1, newchunk)
		case 2:
			chunks2=append(chunks2, newchunk)
		case 3:
			chunks3=append(chunks3, newchunk)
		}
	}
	fmt.Println("sending chunks:")
	/*fmt.Print("chunks1: ")
	fmt.Println(chunks1)
	fmt.Print("chunks2: ")
	fmt.Println(chunks2)
	fmt.Print("chunks3: ")
	fmt.Println(chunks3)*/
	fmt.Println("IDA: ",IDA)
	fmt.Println("my nodeId: ",server.NodeId)
	//funcion definida en client_data.go: SendChunksToDataNode(chunks1)
	switch server.NodeId{
	case 1://soy 1
		if IDA==2{//A es 2 y  B es 3
			err=server.SendChunksToDataNode(chunks2,server.OtherDataNodeA)
			if err!=nil{
				return err
			}
			err=server.SendChunksToDataNode(chunks3,server.OtherDataNodeB)
			if err!=nil{
				return err
			}
		} else {//A es 3 y B es 2
			err=server.SendChunksToDataNode(chunks3,server.OtherDataNodeA)
			if err!=nil{
				return err
			}
			err=server.SendChunksToDataNode(chunks2,server.OtherDataNodeB)
			if err!=nil{
				return err
			}
		}
		server.SaveChunks(chunks1)
		//por ultimo, me "quedo con el resto"
	case 2:
		if IDA==1{//A es 1 y B es 3
			err=server.SendChunksToDataNode(chunks1,server.OtherDataNodeA)
			if err!=nil{
				return err
			}
			err=server.SendChunksToDataNode(chunks3,server.OtherDataNodeB)
			if err!=nil{
				return err
			}
		} else {//A es 3 y B es 2
			err=server.SendChunksToDataNode(chunks3,server.OtherDataNodeA)
			if err!=nil{
				return err
			}
			err=server.SendChunksToDataNode(chunks1,server.OtherDataNodeB)
			if err!=nil{
				return err
			}
		}
		//por ultimo, me "quedo con el resto"
		server.SaveChunks(chunks2)
	case 3:
		if IDA==1{//A es 1 y B es 2
			err=server.SendChunksToDataNode(chunks1,server.OtherDataNodeA)
			if err!=nil{
				return err
			}
			err=server.SendChunksToDataNode(chunks2,server.OtherDataNodeB)
			if err!=nil{
				return err
			}
		} else {//A es 2 y B es 1
			err=server.SendChunksToDataNode(chunks2,server.OtherDataNodeA)
			if err!=nil{
				return err
			}
			err=server.SendChunksToDataNode(chunks1,server.OtherDataNodeB)
			if err!=nil{
				return err
			}
		}
		//por ultimo, me "quedo con el resto"
		server.SaveChunks(chunks3)//funcion definida en client_data.go
	default:
		fmt.Println("node id not recognized")
	}

	fmt.Println("done")

	//TODO: mandar info a namenode
	return nil

}

func (server *Server) DistributedRequest(directions []*data_data.OrderReq) (error, bool){
//en la versión "centralizada", lo unico que cambiaría es que el request se le hace al name node y no a los otros datanodes
	//le pregunto al otro datanode A
	fmt.Println("sending the requests to node A")
	err,req:=server.RequestOrdersToNode(directions,server.OtherDataNodeA)
	if err!=nil{
		return err,false
	}
	if req==false{//se reachazo la propuesta de parte del nodo A
		return nil,false
	}
	fmt.Println("sending the requests to node B")
	err,req=server.RequestOrdersToNode(directions,server.OtherDataNodeB)
	if err!=nil{
		return err,false
	}
	if req==false{//se reachazo una propuesta
		return nil,false
	}
	return nil,true
}

func (server *Server) RequestOrdersToNode(directions []*data_data.OrderReq, cli data_data.DataDataClient) (error,bool) {
	fmt.Println("requesting order to other node...")
	stream,err:=cli.RequestOrder(context.Background())
	if err!=nil{
		return err,false
	}
	for _,data:=range directions{
		if err := stream.Send(data); err != nil {
			return err,false
		}
	}
	reply,err:=stream.CloseAndRecv()
	if reply.ResCode==data_data.OrderResCode_Yes{
		fmt.Println("request accepted")
		return nil, true
	} else {
		fmt.Println("request rejected")
		return nil, false
	}

}

