package internal

import (
	"context"
	"fmt"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Directions struct {
	Lat float64 `bson:"lat" json:"lat"`
	Lng float64 `bson:"lng" json:"lng"`
}

type Route struct {
	ID           string       `bson:"_id" json:"id"`
	Distance     int          `bson:"distance" json:"distance"`
	Directions   []Directions `bson:"directions" json:"directions"`
	FreightPrice float64      `bson:"freight_price" json:"freight_price"`
}

func NewRoute(id string, distance int, directions []Directions) Route {
	return Route{
		ID:         id,
		Distance:   distance,
		Directions: directions,
	}
}

type RouteService struct {
	mongo          *mongo.Client
	FreightService *FreightService
}

func NewRouteService(mongo *mongo.Client, freightService *FreightService) *RouteService {
	return &RouteService{
		mongo:          mongo,
		FreightService: freightService,
	}
}

type FreightService struct{}

func NewFreightService() *FreightService {
	return &FreightService{}
}

func (fs *FreightService) Calculate(distance int) float64 {
	return math.Round((float64(distance)*0.15+0.3)*100) / 100
}

func (rs *RouteService) CreateRoute(ctx context.Context, route Route) (Route, error) {
	opCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	freightCost := rs.FreightService.Calculate(route.Distance)
	route.FreightPrice = freightCost
	fmt.Printf("Calculated freight cost: %.2f\n", freightCost)

	update := bson.M{
		"$set": bson.M{
			"distance":      route.Distance,
			"directions":    route.Directions,
			"freight_price": freightCost,
		},
	}

	filter := bson.M{"_id": route.ID}

	// Upsert option to insert if not exists
	opts := options.Update().SetUpsert(true)

	_, err := rs.mongo.Database("routes").Collection("routes").UpdateOne(opCtx, filter, update, opts)

	return route, err
}

func (rs *RouteService) GetRoute(ctx context.Context, id string) (Route, error) {
	opCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var route Route
	filter := bson.M{"_id": id}
	err := rs.mongo.Database("routes").Collection("routes").FindOne(opCtx, filter).Decode(&route)
	return route, err
}
