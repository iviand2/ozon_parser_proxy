try:
    proxies = ['1', '2', '1', '2', '1', '2']
    # with open('proxies.csv', 'r') as file:
    #     proxies = file.read().split('\n')
    #     proxies = [c for c in proxies if c]
    # if not proxies:
    #     raise ValueError('Файл со списком прокси найден, но пуст.\nПожалуйста, обратитесь к администратору за новым.')
except FileNotFoundError:
    raise FileNotFoundError('Файл "proxies.csv" со списком прокси не найден.\nУбедитесь, что вы не удалили его по '
                            'ошибке, либо обратитесь к администратору за новым.')

