scala_library(
  name='gateway',
  dependencies=[
    'src/jvm/fr/cnrs/liris/privamov/core/model',
    'src/jvm/fr/cnrs/liris/privamov/core/model:json',
    'src/jvm/fr/cnrs/liris/privamov/core/lppm',
    'src/jvm/fr/cnrs/liris/common/geo',
    'src/jvm/fr/cnrs/liris/common/geo:json',
    'src/jvm/com/twitter/querulous',
    '3rdparty/jvm/com/twitter/finatra:http',
    '3rdparty/jvm/com/twitter/finatra:httpclient',
    scoped('3rdparty/jvm/org/postgresql', scope='runtime'),
  ],
  sources=rglobs('*.scala')
)

jvm_binary(
  name='bin',
  dependencies=[
    '3rdparty/jvm:logback',
    ':gateway',
  ],
  resources=[
    'src/resources/fr/cnrs/liris/privamov/svc:logback',
  ],
  main='fr.cnrs.liris.privamov.gateway.PrivamovGatewayServerMain',
  basename='privamov-gateway',
)
