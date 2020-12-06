from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import logging
import config as cfg


class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_company(self, company_name):
        with self.driver.session() as session:
            query = (
                "CREATE (c1:Company { name: $company_name })"
            )
            result = session.run(query, company_name=company_name)
            print(f"Created company named {company_name}")

    def create_worker(self, name, surname, email):
        with self.driver.session() as session:
            query = (
                "Create (w:Worker { name: $name, surname: $surname, email: $email })"
            )
            session.run(query, name=name, surname=surname, email=email)
            print(f"Created worker {name} {surname}")

    def delete_worker(self, name, surname, email):
        with self.driver.session() as session:
            query = (
                "MATCH (dw:Worker) WHERE dw.name = $name and dw.surname=$surname and dw.email=$email\n"
                "DETACH DELETE dw"
            )
            session.run(query, name=name, surname=surname, email=email)
            print(f"Deleted worker {name} {surname}")

    def create_relationship(self, worker_name, surname, email, company_name):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_relationship, worker_name, surname, email, company_name)
            for record in result:
                print(f"Created friendship between: {record['p1']}, {record['p2']}")

    @staticmethod
    def _create_and_return_relationship(tx, worker_name, surname, email, company_name):
        query = (
            "MATCH (w:Worker) WHERE w.name = $worker_name and w.surname = $surname and w.email = $email "
            "MATCH (c:Company) WHERE c.name = $company_name "
            "CREATE (w)-[:WorksIn]->(c) "
        )
        result = tx.run(query, worker_name=worker_name, surname=surname, email=email, company_name=company_name)
        try:
            return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
                    for record in result]
        except ServiceUnavailable as exception:
            logging.error(f"{query} raised an error: \n {exception}")
            raise

    def find_worker(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_worker, person_name)
            for record in result:
                print(f"Found person: {record}")

    @staticmethod
    def _find_and_return_worker(tx, person_name):
        query = (
            "MATCH (p:Worker) "
            "WHERE p.name = $person_name "
            "RETURN p.surname AS surname"
        )
        result = tx.run(query, person_name=person_name)
        return [record["surname"] for record in result]


if __name__ == "__main__":
    scheme = "neo4j"
    host_name = "localhost"
    port = 7687
    url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
    user = cfg.LOGIN
    password = cfg.PASSWORD
    app = App(url, user, password)
    app.create_company("My_company")
    app.create_worker("James", "Wilson", "jameswilson@gmail.com")
    app.create_worker("Robert", "Chase", "robertchase@gmail.com")
    app.find_worker("James")
    app.delete_worker("James", "Wilson", "jameswilson@gmail.com")
    app.create_relationship("Robert", "Chase", "robertchase@gmail.com", "My_company")
    app.close()
