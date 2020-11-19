package main

import(
	transform "./transform"
	grpc "google.golang.org/grpc"
	client_data "../grpc/client_data/client_data"
	"fmt"
	"context"
	"log"
	"sort"

)

func SetDataNodeConnection(id string)(client_data.ClientDataClient, *grpc.ClientConn){
	entry:
		fmt.Println("ingrese dirección IP del servidor del data node "+id+" (en el formato: 255.255.255.255)")
		var IPaddr string
		fmt.Scanln(&IPaddr)
		fmt.Println("ingrese el numero de puerto en el que el data node "+id+" está escuchando")
		var PortNum string
		fmt.Scanln(&PortNum)

		CompleteAddr:=IPaddr+":"+PortNum
		fmt.Println(CompleteAddr)
		conn, err:=grpc.Dial(CompleteAddr,grpc.WithInsecure(),grpc.WithBlock())
		//defer conn.Close()
	if err!=nil{
		goto entry
	}
	cdn:=client_data.NewClientDataClient(conn)
	fmt.Println("created dataclient")
	return cdn, conn
}


func UploadFileToDataNodes(cdn client_data.ClientDataClient, fileName string) error{
	fmt.Println("attempting to download chunks")
	chunks,err:=transform.FileToChunks(uploadPath,fileName)
	if err!=nil{
		return err
	}
	fmt.Println("invoking stream")
	stream, err:=cdn.UploadFile(context.Background())
	if err!=nil {
		return err
	}
	fmt.Println("stream invoked")
	data:=&client_data.UploadReq{
		Req: &client_data.UploadReq_FileName{
			FileName: fileName,
		},
	}
	fmt.Println("sending filename")
	if err := stream.Send(data); err != nil {
		return err
	}
	fmt.Println("sending chunks")
	for i,chunk:=range chunks{
		data:=&client_data.UploadReq{
			Req: &client_data.UploadReq_DataChunk{
				DataChunk: &client_data.Chunk{
					Content: chunk,
					ChunkId: int64(i),
				},
			},
		}
		if err := stream.Send(data); err != nil {
			return err
		}
	}
	reply, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	log.Printf("Route summary: %v", reply)

	return nil
}

func DownloadFileFromDataNodes(fileName string, cdn1 client_data.ClientDataClient) error {
	//actualmente solo manda a un solo nodo
	fmt.Println("invoking stream")
	stream, err:=cdn1.DownloadFile(context.Background())
	fmt.Println("creating filename")
	DonwnloadFileName:=&client_data.DownloadReq{
		Req: &client_data.DownloadReq_FileName{
			FileName: fileName,
		},
	}
	if err!=nil{
		return err
	}
	fmt.Println("sending filename")
	if err:=stream.Send(DonwnloadFileName); err!=nil{
		return nil
	}

	chunksData:=[][]byte{}
	chunks:=[]client_data.Chunk{}
	//TODO: pedirle el verdadero orden al datanode
	for i:=0;i<7;i++{
		fmt.Printf("requesting chunk of id: %d \n",i)
		chunkReq:=&client_data.DownloadReq{
			Req: &client_data.DownloadReq_ChunkId{
				ChunkId: int64(i),
			},
		}
		fmt.Println("sending request")
		if err:=stream.Send(chunkReq); err!=nil{
			return nil
		}
		fmt.Println("sended, receiving response")
		downloadRes,err:=stream.Recv()
		if err!=nil{
			return err
		}
		fmt.Println("response receiving, adding to chunks slice")
		chunks=append(chunks,*downloadRes)	
	}
	//ordenar los chunks por id
	fmt.Println("all chunks received, ordering by id")
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].ChunkId < chunks[j].ChunkId
	})
	fmt.Println("chunks:")
	for _,chunk:=range chunks{
		fmt.Println(chunk.ChunkId)
		chunksData=append(chunksData,chunk.Content)
	}

	fmt.Println("merging bytes to file")
	err=transform.ChunksToFile(chunksData,fileName,downloadPath)
	if err!=nil{
		return err
	}

	fmt.Println("done! :)")
	return nil
}

var(
	downloadPath string 
	uploadPath string
	//cdn1 client_data.ClientDataClient
	//cdn2 client_data.ClientDataClient
	//cdn3 client_data.ClientDataClient
	//cnn client_name.ClientNameClient
	
)


func main(){

	downloadPath="client/filesdown"
	uploadPath="client/filesup"

	cdn1, connd1:=SetDataNodeConnection("1")
	defer connd1.Close()
	fmt.Println(cdn1)



	fileName:="Dracula-Stoker_Bram.pdf"
	err:=UploadFileToDataNodes(cdn1,fileName)
	if err!=nil{
		log.Fatalf("error subiendo archivos: %v",err)
	}

	err=DownloadFileFromDataNodes(fileName,cdn1)
	if err!=nil{
		log.Fatalf("error descargando archivo: %v",err)
	}
	
	//err=transform.ChunksToFile(chunks,fileName,downloadPath)
	
}