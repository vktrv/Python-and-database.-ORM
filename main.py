import sqlalchemy
from sqlalchemy.orm import sessionmaker
import os
import json
from models import create_tables, Publisher, Book, Shop, Stock, Sale

system = 'postgresql'
login = 'postgres'
password = os.getenv('PASSWORD')
host = 'localhost'
port = 5432
name_db = 'book_store'
DSN = f'{system}://{login}:{password}@{host}:{port}/{name_db}'

engine = sqlalchemy.create_engine(DSN)

Session = sessionmaker(bind=engine)
session = Session()

create_tables(engine)


def insert_table():
    with open('test_data.json', 'r') as db:
        data = json.load(db)
    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale
        }[record['model']]
        session.add(model(id=record.get('pk'), **record.get('fields')))
    session.commit()


def filter_publisher(name_publisher):
    if name_publisher.isnumeric():
        return name_publisher

    else:
        id_publisher = session.query(Publisher.id).filter(Publisher.name == name_publisher).scalar()
        return id_publisher


def selection_publisher(id_publisher):
    for result in session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).join(Sale.stock). \
            join(Stock.shop).join(Stock.book).filter(Book.id_publisher == id_publisher).all():
        print_list = list(result)
        print(f'{print_list[0]}|\t'
              f'{print_list[1]}|\t'
              f'{int(print_list[2])}|\t'
              f'{print_list[3].strftime("%d-%m-%Y")}')
    session.commit()


if __name__ == '__main__':
    insert_table()
    selection_publisher(filter_publisher(input('Enter the publisher"s name or number: ')))

    session.close()