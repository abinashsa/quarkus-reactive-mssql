package org.acme;

import io.quarkus.reactive.datasource.ReactiveDataSource;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.mssqlclient.MSSQLPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.acme.model.Fruit;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;


@Path("fruits")
public class FruitResource {
    @Inject
    @ReactiveDataSource("datasource1")
    MSSQLPool client;

    @POST
    @Path("")
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<Response> create(Fruit fruit) {
        return client.preparedQuery("INSERT INTO fruits(name) OUTPUT INSERTED.ID VALUES (@p1)")
                .execute(Tuple.of(fruit.getName()))
                .onItem().transform(rows -> rows.iterator().next().getLong("ID"))
                .onItem().transform(id -> Response.created(URI.create("/fruits/" + id)).build());
    }

    @GET
    @Path("{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> getSingle(Long id) {
        return client.preparedQuery("SELECT id, name FROM fruits WHERE id = @p1")
                .execute(Tuple.of(id))
                .onItem().transform(rows -> {
                    RowIterator<Row> iterator = rows.iterator();
                    if (!iterator.hasNext()) {
                        return Response.status(Response.Status.NOT_FOUND).build();
                    }
                    Row row = iterator.next();
                    Fruit fruit = new Fruit();
                    fruit.setId(row.getLong("id"));
                    fruit.setName(row.getString("name"));
                    return Response.ok(fruit).build();
                });
    }

    @GET
    @Path("")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> getAll() {
        return client.preparedQuery("SELECT * FROM fruits")
                .execute()
                .onItem().transform(rows -> {
                    List<Fruit> fruits = new ArrayList<>();
                    for (Row row : rows) {
                        Fruit fruit = new Fruit();
                        fruit.setId(row.getLong("id"));
                        fruit.setName(row.getString("name"));
                        fruits.add(fruit);
                    }
                    return fruits;
                }).onItem().transform(fruits -> Response.ok(fruits).build());
    }

}
