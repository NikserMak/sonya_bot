# Вопросы для утреннего опроса
SURVEY_QUESTIONS = [
    {
        'text': 'Во сколько вы легли спать вчера вечером?',
        'key': 'bedtime',
        'type': 'time',
        'options': None
    },
    {
        'text': 'Во сколько вы проснулись сегодня утром?',
        'key': 'wakeup_time',
        'type': 'time',
        'options': None
    },
    {
        'text': 'Сколько часов вы спали? (приблизительно)',
        'key': 'sleep_duration',
        'type': 'float',
        'options': None
    },
    {
        'text': 'Сколько раз вы просыпались ночью?',
        'key': 'awakenings',
        'type': 'int',
        'options': None
    },
    {
        'text': 'Как вы оцениваете качество своего сна? (1 - очень плохо, 10 - отлично)',
        'key': 'sleep_quality',
        'type': 'int',
        'options': list(range(1, 11))
    },
    {
        'text': 'Как вы себя чувствуете после пробуждения? (1 - очень плохо, 10 - отлично)',
        'key': 'mood_morning',
        'type': 'int',
        'options': list(range(1, 11))
    },
    {
        'text': 'Насколько вы чувствовали стресс перед сном? (1 - совсем нет, 10 - очень сильно)',
        'key': 'stress_level',
        'type': 'int',
        'options': list(range(1, 11))
    },
    {
        'text': 'Занимались ли вы физическими упражнениями вчера? Если да, сколько минут?',
        'key': 'exercise',
        'type': 'int',
        'options': None
    },
    {
        'text': 'Сколько чашек кофе/чая с кофеином вы выпили вчера?',
        'key': 'caffeine',
        'type': 'int',
        'options': None
    },
    {
        'text': 'Употребляли ли вы алкоголь вчера? Если да, сколько порций?',
        'key': 'alcohol',
        'type': 'int',
        'options': None
    },
    {
        'text': 'Сколько времени вы провели перед экранами (телефон, компьютер, ТВ) перед сном? (в минутах)',
        'key': 'screen_time',
        'type': 'int',
        'options': None
    },
    {
        'text': 'Хотите что-то добавить о своем сне или самочувствии? (необязательно)',
        'key': 'notes',
        'type': 'text',
        'options': None
    }
]