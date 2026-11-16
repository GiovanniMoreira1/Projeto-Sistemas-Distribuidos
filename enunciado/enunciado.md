# Projeto: Sistema para troca de mensagem instantânea

O Bulletin Board System (BBS) foi desenvolvido em no final da década de 1970 e permitia que usuários se conectassem aos servidores para terem acesso a notícias, participarem de discussões e trocarem mensagens com outros usuários. No final da década de 1980, o Internet Relay Chat (IRC) foi desenvolvido para tentar substituir uma parte do BBS, trazendo algumas das funcionalidades. Enquanto servidores de BBS existiram até a década de 1990, os de IRC ainda existem, mas perdendo usuários com o tempo e apesar destes serviços não serem tão usados como no passado, eles serviram como base de muitos serviços para troca de mensagens que são usados atualmente.

A proposta deste projeto é desenvolver uma versão simples de um destes sistemas usando o que estudamos na disciplina de sistemas distribuídos.
 
O projeto que será desenvolvido deverá permitir que usuários (bots) postem em canais públicos, sendo que todas as interações dos usuários com o serviço deverão ser armazenados em disco, permitindo que o usuário recupere mensagens anteriores, que servidores sejam adicionados e removidos sem parada do serviço.

O desenvolvimento deste projeto terá algumas partes padronizadas (e.g., uso de containers para testes, biblioteca para troca de mensagem, quantidade de linguagens que devem ser usadas) e algumas partes que deverão ser escolhidas pelo desenvolvedor (e.g, linguagem utilizada, UI, formato de armazenamento dos dados).

## Partes padronizadas do Projeto

As partes padronizadas do projeto servem para garantir que todos usarão os padrões apresentados nas aulas, verificando na prática o que foi apresentado na teoria. As partes padronizadas são:
- uso do ZeroMQ para troca de mensagens;
- uso de containers (e.g., Docker ou Podman) e orquestração (e.g., Docker Compose) para execução do projeto, usando a rede gerada pela orquestração para realizar a troca de mensagens entre os serviços (não será necessário executar o projeto em mais do que um computador);
- a forma de apresentação do projeto também será padronizada para facilitar a validação da entrega;
- o projeto não deve depender de interação com o usuário para funcionar. A execução do projeto deve ocorrer pelo orquestrador e o usuários do sistma devem ser bots que enviam mensagens para os servidores;
- tanto cliente como servidor devem exibir uma mensagem quando realizam o envio ou recebimento de uma nova mensagem. A mensagem apresentada na tela deve conter todo o conteúdo da mensgagem enviada/recebida formatado de uma forma que fique fácil de acompanhar as trocas de mensagens.

## Partes em que a escolha é livre
- o ambiente de desenvolvimento e de apresentação do projeto;
- o formato de armazenamento dos dados (i.e., mensagens trocadas entre os usuários e serviços);
- a linguagem de programação usada é livre, **porém C++ sem orientação a objetos será considerado como C e TypeScript e JavaScript serão consideradas a mesma linguagem por TS ser compilada para JS**. Antes de iniciar o projeto, confirme se está tudo correto para não perder pontos na avaliação final.

## Grupos
- O projeto pode ser individual, mas deve usar pelo menos 2 linguagens diferentes;
- O projeto pode ser feito em grupos de qualquer tamanho, porém para cada integrante deverá ser adicionada uma nova linguagem que deverá ser usada para desenvolver **todas** as partes do projeto. A entrega (e apresentação) do projeto deve mostrar todos os clientes e servidores sendo executados ao mesmo tempo e deve permitir a troca de mensagens entre todos os serviços.

Exemplos:
1. Se feito sozinho, pode ser usado Python para o servidor e Java para o cliente.
2. Se feito em dupla, um integrante pode usar Python para implementar uma versão do cliente e do servidor e o outro pode usar Java para implementar cliente e servidor. Neste caso, o servidor desenvolvido em Python deve trocar mensagens com o servidor em Java e também conseguir responder as mensagens do cliente em Java. O mesmo serve para o servidor implementado em Java considerando o cliente e servidor em Python.
3. Se o grupo tiver $N$ integrantes, devem ser implementados $N$ clientes e $N$ servidores em $N$ linguagens diferentes, mantendo a troca de mensagem entre todos os clientes e servidores.

## Entrega do projeto
Para facilitar o semestre e não sobrecarregar as últimas semanas de aula, o projeto terá entregas parciais pelo Moodle com o prazo máximo de 1 semana após a liberação do enunciado sendo que cada parte deve ser desenvolvida em uma branch separada. A entrega no Moodle sendo um arquivo zip com o código desenvolvido até a parte específica e o link para a branch em que a parte foi desenvolvida.
