from model.UserStocks import UserStocks
from repository.Repository import GenericRepository
from service.Interceptor import Interceptor

generic_repository = GenericRepository()


class InvestmentHandler(Interceptor):
    def __init__(self):
        super().__init__()

    def add_movement(self, movement):
        movement = generic_repository.add_user_id(movement)
        movement_type = generic_repository.get_object("movement_types", ["id"], {"id": movement['movement_type']})
        coef = movement_type['coefficient']
        try:
            stock = {'ticker': movement['ticker'], 'quantity': movement['quantity'], 'avg_price': movement['price'],
                     'user_id': movement['user_id']}
            if generic_repository.exist_by_key("user_stocks", ["ticker"], movement):
                user_stock = generic_repository.get_object("user_stocks", ["ticker"], movement)
                total_value = float(user_stock['quantity'] * user_stock['avg_price'])

                total_value += movement['quantity'] * movement['price'] * float(coef)
                quantity = user_stock['quantity'] + movement['quantity'] * coef
                if quantity != 0:
                    avg_price = total_value / float(quantity)
                else:
                    avg_price = 0
                user_stock['quantity'] = quantity
                user_stock['avg_price'] = avg_price
                generic_repository.update("user_stocks", ["user_id", "ticker"], user_stock)
                generic_repository.insert("user_stocks_movements", movement)
            else:
                generic_repository.insert("user_stocks", stock)
        except Exception as e:
            print(e)
