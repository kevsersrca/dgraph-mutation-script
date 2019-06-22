package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"google.golang.org/grpc"
)

type CancelFunc func()

func getDgraphClient() (*dgo.Dgraph, CancelFunc) {
	ip := ""
	conn, err := grpc.Dial(ip, grpc.WithInsecure())
	if err != nil {
		log.Fatal("While trying to dial gRPC")
	}

	dc := api.NewDgraphClient(conn)
	dg := dgo.NewDgraphClient(dc)
	//ctx := context.Background()

	return dg, func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error while closing connection:%v", err)
		}
	}
}

type Starring struct {
	Actor     []Actor     `json:"performance.actor,omitempty"`
	Character []Character `json:"performance.character,omitempty"`
	Film      []Movie     `json:"performance.film,omitempty"`
}

type Genre struct {
	Uid  string `json:"uid,omitempty"`
	Name string `json:"name@en,omitempty"`
}

type Film struct {
	Uid  string `json:"uid,omitempty"`
	Name string `json:"name@en,omitempty"`
}

type Actor struct {
	Uid  string  `json:"uid,omitempty"`
	Name string  `json:"name@en,omitempty"`
	Film []Movie `json:"performance.film,omitempty"`
}

type Director struct {
	Uid  string  `json:"uid,omitempty"`
	Name string  `json:"name@en,omitempty"`
	Film []Movie `json:"performance.film,omitempty"`
}

type Character struct {
	Uid  string `json:"uid,omitempty"`
	Name string `json:"name@en,omitempty"`
}

type Movie struct {
	Uid                string     `json:"uid,omitempty"`
	Name               string     `json:"name@en,omitempty"`
	NameDe             string     `json:"name@de,omitempty"`
	NameTr             string     `json:"name@tr,omitempty"`
	InitialReleaseDate time.Time  `json:"initial_release_date,omitempty"`
	Genre              []Genre    `json:"genre,omitempty"`
	Starring           []Starring `json:"starring,omitempty"`
	Director           []Director `json:"director.film,omitempty"`
	Actor              []Actor    `json:"actor.film,omitempty"`
}

func main() {
	t, _ := time.Parse(time.RFC3339, "1998-11-27T00:00:00Z")
	dg, cancel := getDgraphClient()

	defer cancel()

	p := Movie{
		Name:   "Everything's Gonna Be Great",
		NameDe: "Alles wird gut",
		NameTr: "Herşey Çok Güzel Olacak",
		Genre: []Genre{{
			Name: "Comedy",
		}, {
			Name: "Drama",
		}},
		Starring: []Starring{
			{
				Character: []Character{{
					Name: "Altan Camli",
				}},
				Actor: []Actor{{
					Name: "Cem Yılmaz",
				}},
			},
			{
				Character: []Character{{
					Name: "Nuri Camli",
				}},
				Actor: []Actor{{
					Name: "Mazhar Alanson",
				}},
			},
			{
				Character: []Character{{
					Name: "Ayla Camli",
				}},
				Actor: []Actor{{
					Name: "Ceyda Düvenci",
				}},
			},
			{
				Character: []Character{{
					Name: "Cevat Camli",
				}},
				Actor: []Actor{{
					Name: "Selim Nasit",
				}},
			},
			{
				Character: []Character{{
					Name: "Nusret",
				}},
				Actor: []Actor{{
					Name: "Mustafa Uzunyilmaz",
				}},
			},
		},
		Director: []Director{
			{
				Name: "Ömer Vargi",
			},
		},
		InitialReleaseDate: t,
	}

	ctx := context.Background()
	mu := &api.Mutation{
		CommitNow: true,
	}
	pb, err := json.Marshal(p)
	if err != nil {
		log.Fatal(err)
	}

	mu.SetJson = pb
	assigned, err := dg.NewTxn().Mutate(ctx, mu)
	if err != nil {
		log.Fatal(err)
	}

	// Assigned uids for nodes which were created would be returned in the assigned.Uids map.
	puid := assigned.Uids["blank-0"]
	const q = `query Me($id: string){
		me(func: uid($id)) {
			uid
			name@en
			name@tr
			initial_release_date
			director{
				name@en
			}
			starring{
				performance.actor{
					name@en
				}
				performance.character{
					name@en
				}
			}
		}
	}`

	variables := make(map[string]string)
	variables["$id"] = puid
	fmt.Println(puid)
	resp, err := dg.NewTxn().QueryWithVars(ctx, q, variables)
	if err != nil {
		log.Fatal(err)
	}

	type Root struct {
		Me []Movie `json:"me"`
	}

	var r Root
	err = json.Unmarshal(resp.Json, &r)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(resp.Json))
}
