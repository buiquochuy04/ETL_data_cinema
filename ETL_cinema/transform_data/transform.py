import pandas as pd
def transformed_cinema(df):
  df['note'] = df['note'].astype(str).str.replace(r"\s++", " ", regex=True)
  df['status'] = df['status'].str.strip()
  df.drop(columns=["id"], inplace=True)
  return df
def transformed_room(df):
  df = df.drop_duplicates(subset=['name', 'cinema_id'])
  df.drop(columns=["id"], inplace=True)
  return df
def transformed_seat(df):
  df = df.drop_duplicates(subset=['room_id', 'position'])
  df.drop(columns=["id"], inplace=True)
  return df
def transformed_movie(df_movie):
  df_movie['description'] = df_movie['description'].str.replace(r"\s+", " ", regex= True)
  df_movie['releaseDate'] = pd.to_datetime(df_movie['releaseDate'], dayfirst=True).dt.strftime('%Y-%m-%d')
  df_movie = df_movie.where(pd.notna(df_movie), None)
  df_movie['ageRating'] = df_movie['ageRating'].str.split('-').str[0]
  df_movie['trailer'] = df_movie['trailer'].str.replace(r"\s", "")
  df_movie['poster'] = df_movie['poster'].str.replace(r"\s", "")
  df_movie.drop(columns=["id"], inplace=True)
  return df_movie

def transformed_showtime(df_showtime):
  df_showtime = df_showtime[df_showtime['movie_id'] != 0]
  df_showtime.drop(columns=["id"], inplace=True)
  return df_showtime

def transformed_customer(df_customer):
  df_customer = df_customer.drop_duplicates(subset=['username'])
  df_customer.drop(columns=["id"], inplace=True)
  return df_customer

def transformed_employee(df_employee):
  df_employee = df_employee.drop_duplicates(subset=['username'])
  df_employee.drop(columns=["id"], inplace=True)
  return df_employee

def transformed_discount(df_discount):
  df_discount.drop(columns=["id"], inplace=True)
  return df_discount

def transformed_product(df_product):
  df_product.drop(columns=["id"], inplace=True)
  return df_product

def transformed_invoice(df_invoice):
  df_invoice = df_invoice.where(pd.notna(df_invoice), None)
  df_invoice.drop(columns=["id"], inplace=True)
  return df_invoice

def transformed_ticket(df_ticket):
  df_ticket.drop(columns=["id"], inplace=True)
  df_ticket = df_ticket.drop_duplicates(subset=['seat_id'])
  return df_ticket

def transformed_productusage(df_productusage):
  df_productusage.drop(columns=["id"], inplace=True)
  return df_productusage