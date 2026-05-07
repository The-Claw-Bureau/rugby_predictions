import os
import requests
import json
import logging

import azure.functions as func


import .my_functions



db_password = os.environ["db_password"]
db_username = os.environ["db_username"]


channels_dict = {
    'production_notifications': 'https://blackboxcapital.webhook.office.com/webhookb2/12832e5c-9eb6-4b76-a26a-ff293a2c9663@4de4e7e1-bc7f-498b-8438-ec890555edb6/IncomingWebhook/bbbb9136ce65431791647c4e88f96d33/42c482d2-d77f-431f-b445-20ca3690f2a4'
    }


POSTGRESQL_PARAMS = {
  'username': db_username,
  'pass': db_password,
  'host': "bbdb-master.postgres.database.azure.com",
  'DB': "bbc"
    }


international_rankings = {
    "cae79170-e3ef-4329-9903-c10186a49cec" : 270.531289753092,
    "bf41a52c-94f3-426a-b707-f2f11cb01de8" : 262.6964625275002,
    "216fc1fc-f969-462b-8875-b1910c85e482" : 262.500333594012,
    "ee8f8374-1c04-4276-ab75-4bafc7441912" : 254.8639020080243,
    "7d3aef5a-7793-46ba-89b7-cbddf6fe12eb" : 253.4310945724153,
    "294eaf52-dfdd-4918-ad0a-3274f5dbd2cf" : 247.30755460216426,
    "f9413c17-aeb0-4739-b9ba-b9bf52991c68" : 247.2347794000136,
    "33c5dddd-470e-4bbf-888a-41de5eba7f59" : 245.00975061064813,
    "1f4f7424-1bc0-4aed-adc2-6ccfab40fbae" : 243.98002632761163,
    "cfe6acd2-6656-4c02-bfae-28bd6f368cfc" : 242.10353682767337,
    "9a001913-8ea4-454a-8224-7514825912ca" : 238.51489518896864,
    "8b06e788-3af4-4392-bb82-062f18cea1cb" : 228.77813569242878,
    "0f3fc29e-5c6a-4f8f-ad76-38d420f69810" : 227.73865458436666,
    "b2a25dd9-e46d-4673-bda6-76638b4dd5e0" : 225.98819795537295,
    "5776fd4c-0e23-4c79-82e7-c7f4cb9ec9ea" : 222.262648344341,
    "74412498-e87e-42d0-9270-7d5fec902b46" : 220.30282319455296,
    "361d5ba8-4d57-4093-940c-01b36fc274ff" : 215.40287788427787,
    "95c15dd0-52af-47bf-8322-753183c0477d" : 208.87866585716216,
    "9fc3a386-e2c7-4556-a78a-ffe91319b405" : 205.2509102091039,
    "16750364-f193-4c66-85e9-b04a2ae03fa2" : 203.20504662846588,
    "e72cf7c2-876b-41c4-bdd1-d1e4a0ebbd31" : 202.67940172277048,
    "1a492fae-2e3a-4203-90c4-0f6eac201ff3" : 199.47695265923403,
    "a3a64375-a2ae-4342-bbd2-35665c2f31aa" : 188.11899841831925,
    "f6c7df6a-bb15-4053-a4e5-b988f2eed556" : 180.77568958513632,
    "22241244-4a29-42fc-80b8-a92b49fbba82" : 177.72447906761704,
    "84527f85-edeb-4cf0-a3c0-89ff1e5df4dd" : 177.04385203858126,
    "a57b246b-26ce-4fd4-820b-4c91caa3e7e5" : 167.38336177875624,
    "46f2867c-4b46-417a-a118-8c195f90ce02" : 167.23294005805872,
    "6caa9439-17ef-4d7d-9c60-de9d59c2f7e7" : 163.8260216716606,
    "83a024eb-6f8b-4dda-ad67-c49e4898fdb5" : 162.27822488075222,
    "3fe21187-7c48-4ccf-bf9d-34cd7e21ea7d" : 161.7496001003144,
    "ff12aad4-85c2-45a8-a366-f45eb8506809" : 161.52414271456007,
    "6bae6049-8544-4f4e-b72b-05521b2c37b6" : 161.09709915930733,
    "cfdd7fce-43a1-4af6-a4ca-0c2e23d075f9" : 160.71915319427933,
    "e7377542-f91c-45be-a38e-c8dfe43678a4" : 159.06091054561256,
    "db719e32-2b29-40d3-8b62-b05da1bdd93b" : 158.73960573515143,
    "b9148cde-89c8-4e81-8f65-09fe38b15a0f" : 157.786054875649,
    "fcf8cd85-21ea-45e9-b659-cfc44f41cea9" : 156.42468129767366,
    "840e016f-2571-479e-8ce1-4996d6d41987" : 155.42063357150843,
    "a6918a34-b0e2-43c9-990f-cb1d814c600b" : 155.22144950068278,
    "4016a1db-70ea-4a31-9d59-434dc82893cc" : 154.31354953756653,
    "a08e1e60-f1b9-408f-942a-1cad150609e9" : 152.0832477055657,
    "62e8345c-c91c-4bbc-8d26-f151e1424171" : 151.4871549278716,
    "cecc2bb5-19a0-4c6e-80c7-ba79ef3acf35" : 150.68656880447938,
    "4f9e48fc-a34e-474b-a26e-ad1b57cd8880" : 150.40153530533885,
    "9aaa03bb-560d-4911-9167-171b29098c03" : 150.02845828606462,
    "8af0eb19-e0f8-491c-9f70-ce9503b561a7" : 146.99729102200715,
    "c101883b-5108-4a55-985e-4b84d2a3c02d" : 146.8550253324795,
    "bb19921f-d2d9-486a-85ac-50453154687f" : 142.99815087101373,
    "6a753deb-93d1-4451-9bba-21955169e224" : 141.50799495220156,
    "d9a0676e-6902-49fd-8108-fcc78afed1ad" : 141.2198395114673,
    "2f4b0698-1d52-4b3f-a051-3704eb8f982f" : 139.90878738814266,
    "74fb1ee4-f7ee-429c-b9ad-6dd7531b6097" : 139.44929507800913,
    "5b10114c-a0f7-425e-abd3-cc821c166848" : 139.3977766764734,
    "c8511e0a-875a-4544-925e-e414b4a2cbd6" : 138.85197749998602,
    "0828700f-e81f-4b79-b9e4-146e47bd02a6" : 138.4791476037188,
    "3b3ba5f2-bc7c-4d2c-bad9-dbc8c4bda38d" : 136.21854543780327,
    "92261fdf-2983-42f0-b086-e1725377613b" : 135.71144433852797,
    "6f1cdbae-e0d3-4072-a218-d9f20efec921" : 135.58771136357825,
    "5ae92037-d1aa-4a3e-ab6c-54b7f8a5628e" : 134.8461789848428,
    "0b88e7c1-517e-4610-ad50-c3771a5e4531" : 134.04125630299973,
    "cd0d53f5-4a11-4ded-8594-641ef025842d" : 131.94380898614452,
    "e82cba06-a49b-413d-ad62-8e91a5a4ac0a" : 131.63730193052288,
    "62368651-78df-4e34-8041-dbb264cdeb7c" : 131.0607968684225,
    "bd9c9946-aabc-499c-8309-78534629c1bd" : 129.4097348418902,
    "8ba5c2aa-6b0f-481c-989d-8fc3ad0370f3" : 128.99149081436522,
    "de14dbca-cc9c-4cb7-bebf-efae01dcb822" : 127.48771076846144,
    "ce102af7-f595-4990-84db-8f37988bd26e" : 126.67794757475305,
    "3c424a5f-586a-4625-aaa8-22d5ff95fc39" : 125.99916191491872,
    "dce422e9-d1f2-4f70-8db4-f1ee69e9a5e0" : 125.82023924397132,
    "27c96b31-4be6-454b-88bd-5e970473e645" : 125.80224756552967,
    "265db8e9-a59d-4bb7-8858-62fa7170833d" : 124.62833767755313,
    "312497cd-b9d1-4cd7-adf4-b081c29cb6d7" : 124.44348147086419,
    "1ebda97c-0182-453f-8da5-7022b630dea7" : 123.73397659875182,
    "600c8b51-10fb-44aa-ae77-834e3881a3ae" : 123.1830496555568,
    "e6d01bb0-a67a-4195-9156-999343d08697" : 122.73580453894543,
    "f4648662-ae61-45db-9fe8-5b818a11d599" : 121.91678889930125,
    "190f9e23-c24a-4f3f-88e2-0f1dde8543c7" : 121.73935430918232,
    "400f8ef2-9ec1-4a6a-92e5-14b6ddba35e7" : 121.69707797090167,
    "b26274d1-6d81-46c4-a8ca-0038056ad58f" : 121.19286245276098,
    "02ebc964-b4a1-497e-8821-283610635bd5" : 121.05516511164572,
    "17a8fec6-aa93-4807-8ec6-5b6c1ec25b0d" : 120.8815730359892,
    "2e4414c4-43b1-4a1d-b6ca-cece82f0390a" : 119.86430015307373,
    "868a967a-3ac8-41f1-9ec0-4af59209a3a9" : 119.52448149945093,
    "96e85b30-b94c-4fd9-bf82-8eabda0fb8db" : 118.08251319932555,
    "a6905fe3-e6c9-4458-8a45-03a26a73b0f4" : 117.3702290679179,
    "f1bbf189-7206-4192-8b9c-8edc271a8d20" : 117.36175699612654,
    "7a25b6c2-b2c2-4677-ac60-1f8131d61a07" : 117.23369137911129,
    "bd12b013-350a-4d33-9c84-e2ecda14f418" : 116.97099254581401,
    "50914914-43ba-42f8-93d0-e52303562ba6" : 116.8680019111917,
    "8a8a1c21-cdb1-4824-886d-b90ef69085e2" : 116.07467466068154,
    "6db54310-4ea9-42fa-9b4f-601f06aabe83" : 112.76621252999175,
    "f5c70b87-ba49-4824-b2ca-d10dfaf1edb1" : 112.43027184339287,
    "ecf78241-3356-40fd-9d48-52b4056d8473" : 109.63408332718878,
    "b6f38f03-3e4a-41d0-86df-be494b3967af" : 108.24079219216625,
    "2d78c309-36aa-4df1-8763-3d4d56bfb55a" : 107.78785206352754,
    "58966b9c-aebc-4042-a6be-129c10172615" : 106.22823766008682,
    "1c415060-a1d3-401e-bdf5-086328982ee6" : 105.34209703785767,
    "1f829cbd-4c9e-4ef8-a72a-f21c9625e0b5" : 105.10151298717244,
    "8a67eb9b-970d-426b-83cc-67f59221eef7" : 102.14109769557506,
    "a9f1012c-7d0d-4c63-a274-a31fd1abac56" : 102.03758738201476,
    "68e9ddce-2dcd-48e3-8eeb-805ba86c73aa" : 102.00177536673573,
    "a50cc0d4-8c28-4688-8aa1-4a3ab52d97af" : 101.26345951499675,
    "47c150ae-b6f9-4864-b00b-3a72228aff16" : 100.47818770037529,
    "7f714b76-9695-4906-8858-de1131923630" : 96.37686986387892,
    "2dd2dbbc-c332-4652-9f55-fd4d98e798ed" : 95.64384512398674,
    "30255d9a-3f51-4764-a892-c22bdb4b70da" : 93.67847306927693,
    "3f67e978-77ee-45aa-ad67-1ee66c241221" : 93.54191981475445,
    "35b61808-0694-4321-83ee-5b66708d0e5a" : 93.46380306906244,
    "59224544-6010-4265-968a-9b8a358c59eb" : 92.60864583068141,
    "92402401-c5f4-475e-a581-8ff59193c357" : 92.37389532935714,
    "7f12ac33-db8c-4a9d-8807-1a96af3f9443" : 91.23892766861444,
    "43fb73bd-5b2f-4bea-9f3f-b70783497484" : 89.0382079620317,
    "5d01fec8-034e-4639-8674-2cebe4258589" : 87.05936985980472,
    "7220605e-2f27-40e1-a52d-6493ea7304b8" : 84.18651014454024,
    "73352379-f726-4987-a355-463a0ec75dc1" : 82.10887201753671,
    "586460e0-afbb-45d0-84d7-26d06b106520" : 80.97169689996696,
    "bb577965-c80c-46ca-9ed8-bffad88aa293" : 78.05674825859674,
    "cce33035-2831-4fe7-8198-0961c980028d" : 77.31068792701126,
    "5f2d4c7c-119d-4b29-8f40-be1eecd9320d" : 74.7427715529407,
    "be3cfe43-a4af-4bda-815a-a97cab6a4bfd" : 70.19954414162407,
    "b202d6dd-f7e2-4cf7-80eb-14071c4c1af1" : 61.09982388767824,
    "0b92ee08-36f5-4e6a-a3e0-d437b836d3ac" : 60.1324816547296,
    "46c2e844-b771-48f4-b405-d6886a93486b" : 53.52622426075546,
    "1e1ddf89-0c52-448b-9bd1-3208730ae559" : 47.55712894607879,
    "71388112-0253-4552-a48f-5080b8dab061" : 40.772886599343444,
    "716039d6-fcfc-4949-bd31-46d789c84ed3" : 38.61582079150535,
    "f4a63316-66f3-49d6-ad31-754144e1058b" : 25.416164676227453}


international_rankings_women = {"19fb07f1-955c-43f7-9140-047ca0a27f0f" : 242.39338072280987,
    "9f99e503-a583-4709-acc1-eff04e148ce5" : 237.27127523060165,
    "df5575dc-63f8-4549-980b-8a6e96104030" : 229.12303160300922,
    "3d83768f-0207-4387-93eb-d7727bcda166" : 216.36359111435615,
    "ae932176-8290-4fc8-a92a-75f8de57e87b" : 215.61376478796262,
    "95c0a975-15e0-4da8-b02d-04ff39eb021e" : 213.9352094738502,
    "8b48650e-c406-48f4-bdfa-ea7fb05b88e0" : 212.93391294863684,
    "df013f15-33c3-426e-a8a9-9ffe2b16b021" : 211.39583456032352,
    "d046337f-0a39-4096-9d1e-06a94927f1dd" : 195.21608806674567,
    "030888f9-d261-490a-b52a-c2b39b0fc3ae" : 193.83897040260544,
    "acf18456-0cef-4f19-b2ef-393c61e2d187" : 187.98838671817265,
    "6e3994e8-cda9-4946-a613-c15e987546fb" : 186.76146899893314,
    "7bf4d486-8075-42a6-9543-6fc46cb51354" : 185.13838376536359,
    "590711c9-fd7d-4354-aaf6-6d03595cf7f2" : 184.3565408879047,
    "dcc03189-a3ba-43d8-8cfe-d28af5c937fd" : 184.35165548270942,
    "7a37de83-e765-499e-baec-c9841c7a60a1" : 178.73656598617242,
    "e53871dc-305b-4fc8-a3f2-6d1ab35df098" : 175.4780230741793,
    "68d37772-e670-4a85-bf2d-69befacd2776" : 172.8556574380159,
    "2de25f00-9bc7-4b60-b0c9-bbfcbdd2a250" : 172.22850512818252,
    "e08811e5-bdb1-45f5-a5ef-3fb74403b62e" : 171.53225956477257,
    "02e9dc14-cc03-4f0c-a124-70f58037d30a" : 169.99943889440192,
    "718d3f41-906a-4bff-9628-f00d304590e5" : 166.8862484270072,
    "aef43d70-ed03-4dc3-9e72-2121728ba75c" : 166.82657077792612,
    "3039976c-05a8-4953-a9ec-38e471d47df8" : 149.97735692741625,
    "b8d97768-4519-4be8-b6dc-dbb1695f10f3" : 149.84319845779012,
    "09b61b86-abdc-4da0-a500-378da8525a87" : 149.7815910606296,
    "8c36af7f-f512-4d1b-b1d5-cac31db9f769" : 147.8757185263841,
    "3ae0c2af-1b7a-42ac-91b1-c24dafe46816" : 145.47524426958591,
    "b29dd972-aa17-4c18-89de-9dcedb144d25" : 137.29395509759658,
    "8fc4c6d9-7f2d-4711-9abd-300ac453d134" : 136.59819863146888,
    "6bf11bed-5fc1-4e1d-b745-acc2505f6699" : 135.0017667433363,
    "d4464a9e-56b6-45c2-9f7b-322cd08e44fa" : 132.46417283393012,
    "67c23e5a-b9eb-4d71-9496-57a28dce9208" : 130.32623750373827,
    "4c3819ab-78d5-4b8b-9dbb-4d2e930421ea" : 128.92260315693065,
    "63b96c9f-a0b3-490d-a2da-34acf973d1c6" : 124.68642968432688,
    "2c32d712-afe4-4934-9a8e-af2b5e50017d" : 124.24173492862501,
    "c911b5fd-207d-4004-8ada-147ff2f5771a" : 122.56700437034058,
    "f6101db3-5db3-4b85-974f-c02aeeb1c72a" : 122.52429103585038,
    "33b7cfd8-93ce-4bde-87c2-e9c6316ffc7d" : 111.73440410670977,
    "e303a7b1-bd20-4eee-86c2-2ee46ea634e2" : 103.71724817189305,
    "0d8b972f-a34f-4ab4-8290-820f4ece141b" : 99.17196812703068,
    "7fb72287-7b41-4355-86e3-ea8e546f415d" : 91.76179541794515,
    "dc3744d9-0c01-4ab6-817b-ccdc59292f53" : 91.2123729096142,
    "5db95245-16a1-4525-9b4a-200cbb98b472" : 85.83448842762587,
    "93458328-6923-4135-b712-003b97052461" : 82.88457315540145,
    "4a594de1-30b9-4c36-a401-92c078938608" : 74.56303044523703,
    "9be399e8-3147-40f7-9faa-3fdd0af8a3b1" : 71.6501471635801,
    "07631c46-5ac9-4c64-a35d-f774763a0b85" : 65.17696743364813}


new_comp_levels = {'4f0ae53d-8cd4-4d93-9fbe-0ce2df4bb56b':2,
                  'f58adc7b-8883-4ad7-a3ea-7e4b68cd3bc3':3,
                  '1885fb8a-c0f2-4b19-aaaa-111e4a40567e':3.4,
                  'd4c72439-882a-4ac5-9f95-d306ed321ccc':2,
                  '5a15b335-c65a-4878-96b6-41f90e785dad':2.5,
                  'f081b233-551c-4948-8840-29251e1952d3':2,
                  '4d148966-2365-4a01-9448-c1190b7d08c8':2,
                  '35f5e778-1144-44fa-a70f-03743c031bee':3,
                  '7df6de6a-e1a0-4b11-bffd-262c84b74789':4,
                  '0524a4e0-efeb-4780-93b9-be98b2f3fbe2':3,
                  '95ab3fbf-4670-4405-8025-6c65efce65d7':3,
                  '69397a9a-19ef-4d82-bc86-370baf9c53c3':3,
                  '0494931c-b163-11ea-8832-001a7dda7115':4,
                  '283d2a95-fa56-4316-aa1e-15a087349166':3.5,
                  'cd7e1345-a727-4fed-a3a9-617203745ba9':2,
                  '518f3a42-b7d3-4640-a5c8-e0225fd88093':3,
                  '7527e849-f9fd-4b2c-8919-c3d76ee0087e':3,
                  '6fcfb116-d342-4080-8bd8-fd080b462ef4':3,
                  '61df7e56-1300-464e-84f1-54663d44f004':4,
                  'ae3d6158-8a63-4065-8b21-0a1fbc283e18':3,
                  'f4b46636-b162-11ea-af86-001a7dda7115':3.5,
                  '7ad4f3cc-83e4-45f4-8391-94360198d64f':2,
                  '5276f62a-428a-4f02-9f29-7913ef00a5d1':2.6,
                   '1fe03390-c49e-48da-b101-cbece0039968':3,
                   'e9913e66-2b89-489b-9978-f72e53e167b4':3}


other_initial_rankings = {'4ec911de-2240-4bf1-a239-b75bbf036977':270.531289753092,
                          '3f081086-0a7a-4fb1-afd8-4ba48f720565':190,
                        'ee3a03c3-2153-43f5-bf34-dd6e518328bf': 148.63, 
                        'b27166c4-17ee-4d9b-9160-6c7ed5875cb2':162.85,
                         'a2de5edd-f074-4c95-9057-a1a2f058a230':216,
                         '22fe9a0d-42f1-4987-af55-e15556232d0e':170,
                         '2ee0306c-103d-4771-8a18-bb3bc234d5a5':240,
                         '9ab9d258-1e66-45a3-874c-a262db8a1b76':200,
                         '904538c8-4f8b-4a99-ae4d-ad31a12cfda5':180,
                         '6a018e47-b86f-4b69-9b11-5cd61b3ff007':162,
                         '5c856e7b-2e24-4c7a-b37f-2a7e3098b1ab':129,
                         '0953dff9-8850-4041-a921-5f0590374ee8':180,
                         'ed6caa7b-f83b-469b-8fab-0b752e72421b':50,
                         'e6a692fe-80d9-43b2-8bd7-c67f2d075b88':64,
                         '5bbbced0-ae87-11ea-a648-001a7dda7115':64,
                         '46c2e844-b771-48f4-b405-d6886a93486b':64,
                         '3af67b25-52cd-409d-9fb8-16979081d9d8':64,
                         '633e6046-5857-4e41-9243-8bb1c286f817':200,
                         'da82a9ec-4def-4629-895d-08d5e4b2bd2a':120,
                         '3af67b25-52cd-409d-9fb8-16979081d9d8':142,
                         '7220605e-2f27-40e1-a52d-6493ea7304b8':140,
                         '7f12ac33-db8c-4a9d-8807-1a96af3f9443':140,
                         '6caa9439-17ef-4d7d-9c60-de9d59c2f7e7':194,
                         'a4b072df-153b-49f9-8bbd-6b03c7592edc':65,
                         '35f352a1-018b-4181-92a8-776c1007ff94':165}


list_order_int_men = ['pre_game_rank_senior_team_ranking', 
                      'opp_pre_elo',
                      'pre_game_rank_historic_competition_median', 
                      'pre_game_rank_historic_home_competition_group_median', 
                      'pre_game_rank_new_home_competition_group', 
                      'pre_game_rank_int_comp_level_setting',
                      'all_events_median']


list_order_int_women_age = ['pre_game_rank_senior_team_ranking', 
                      'opp_pre_elo',
                      'pre_game_rank_historic_competition_median', 
                      'pre_game_rank_historic_home_competition_group_median', 
                      'pre_game_rank_new_home_competition_group', 
                      'pre_game_rank_int_comp_level_setting',
                      'all_events_median']


list_order_club = ['pre_game_rank_senior_team_ranking',
                   'pre_game_rank_historic_competition_median',
                   'pre_game_rank_historic_home_competition_group_median',
                   'opp_pre_elo',
                   'pre_game_rank_new_home_competition_group',
                   'pre_game_rank_int_comp_level_setting',
                   'all_events_median']


# latest one without pre_delta_diff_adjusted
points_transfer_dict = {'d4c72439-882a-4ac5-9f95-d306ed321ccc':0.5, '4f0ae53d-8cd4-4d93-9fbe-0ce2df4bb56b':0.5, 'f330a8ae-38bf-4ec2-9735-6269f6b14e77':0.6, '34832c47-d30e-40ca-b5c6-4065b8b01715':1.2, '92c3cd86-8fb6-4b0b-8044-da30f89b4c8d':0.6, '7e901bf2-005f-461b-8105-acabc014ff2c':0.6, '11552296-5f73-427f-9308-6e0013a3ff8c':1.3, 'cead6c33-bc0d-4358-86cc-71db61af7d9c':1.3, '7ad4f3cc-83e4-45f4-8391-94360198d64f':0.5, 'ddefeea0-f8bc-4295-93ee-e1113b694bee':1.3, 'f081b233-551c-4948-8840-29251e1952d3':0.4, 'ab1959c0-41be-4e55-8dd7-87f40892520a':1.3, '0524a4e0-efeb-4780-93b9-be98b2f3fbe2':1.1, 'f58adc7b-8883-4ad7-a3ea-7e4b68cd3bc3':0.6, '396d8107-2dac-436e-a9f8-5883991655c0':1.3, 'd4f47c1f-2257-4dbc-8ff6-68666628f456':0.7, '35f5e778-1144-44fa-a70f-03743c031bee':0.7, 'e9913e66-2b89-489b-9978-f72e53e167b4':0.4, '5a15b335-c65a-4878-96b6-41f90e785dad':0.5, '6fcfb116-d342-4080-8bd8-fd080b462ef4':0.9, 'd1ab7c83-e333-423f-8470-237a8467e01d':0.8, '7df6de6a-e1a0-4b11-bffd-262c84b74789':0.9, '1885fb8a-c0f2-4b19-aaaa-111e4a40567e':0.8, '1fe03390-c49e-48da-b101-cbece0039968':0.6, 'c5ec9df5-f62f-48b9-8298-35f95fe64538':0.6, 'e7d6e23d-bd03-4592-ab9e-4ae0695acd79':1.3, '2ac22fec-3cbd-4e5e-8ca7-520c785c3a36':1.3, '183ec923-6902-4c1c-9035-506588249902':1, 'c2f15e2a-41fa-40b4-8779-8156a2cc74e2':0.8, 'dce1cf60-fdce-45f9-83fa-b9708818a623':1.1, '5dab0ba3-08df-49c0-a8b3-34219efb3a6d':0.9, '4e28d4f0-5f4d-4783-b7fd-c47e07374471':1.3, '9597f979-d95b-48ca-8fd3-401357e98e1c': 0.6, 'e5132fce-9605-4ab5-9083-c21a10e8afff': 0.9, '7cbf859b-970e-4a6e-9960-78d1995aced9':0.8}


# First ttransfer but trained on already adjussted pre_delta_diff_aadjusted
points_transfer_dict = {'4f0ae53d-8cd4-4d93-9fbe-0ce2df4bb56b':0.5, 'd4c72439-882a-4ac5-9f95-d306ed321ccc':0.4, 'f330a8ae-38bf-4ec2-9735-6269f6b14e77':0.9, '34832c47-d30e-40ca-b5c6-4065b8b01715':1.2, '92c3cd86-8fb6-4b0b-8044-da30f89b4c8d':0.7, '7e901bf2-005f-461b-8105-acabc014ff2c':0.5, '11552296-5f73-427f-9308-6e0013a3ff8c':1.2, 'cead6c33-bc0d-4358-86cc-71db61af7d9c':1.2, '7ad4f3cc-83e4-45f4-8391-94360198d64f':0.8, 'ddefeea0-f8bc-4295-93ee-e1113b694bee':1.1, 'f081b233-551c-4948-8840-29251e1952d3':0.5, 'ab1959c0-41be-4e55-8dd7-87f40892520a': 1.1, '0524a4e0-efeb-4780-93b9-be98b2f3fbe2':1, 'f58adc7b-8883-4ad7-a3ea-7e4b68cd3bc3':0.7, '396d8107-2dac-436e-a9f8-5883991655c0':1.2, 'd4f47c1f-2257-4dbc-8ff6-68666628f456':0.9, '35f5e778-1144-44fa-a70f-03743c031bee':0.7, 'e9913e66-2b89-489b-9978-f72e53e167b4':0.6, '5a15b335-c65a-4878-96b6-41f90e785dad':0.4, '6fcfb116-d342-4080-8bd8-fd080b462ef4':1, 'd1ab7c83-e333-423f-8470-237a8467e01d':1.1, '7df6de6a-e1a0-4b11-bffd-262c84b74789':1.2, '1885fb8a-c0f2-4b19-aaaa-111e4a40567e':1.1, '1fe03390-c49e-48da-b101-cbece0039968':0.6, 'c5ec9df5-f62f-48b9-8298-35f95fe64538': 0.9, 'e7d6e23d-bd03-4592-ab9e-4ae0695acd79':1.2, '2ac22fec-3cbd-4e5e-8ca7-520c785c3a36': 1.3, '183ec923-6902-4c1c-9035-506588249902':0.8, 'c2f15e2a-41fa-40b4-8779-8156a2cc74e2': 1.3, 'dce1cf60-fdce-45f9-83fa-b9708818a623': 1, '5dab0ba3-08df-49c0-a8b3-34219efb3a6d': 1, '4e28d4f0-5f4d-4783-b7fd-c47e07374471':1.3, '9597f979-d95b-48ca-8fd3-401357e98e1c':0.7, 'e5132fce-9605-4ab5-9083-c21a10e8afff':1.3, '7cbf859b-970e-4a6e-9960-78d1995aced9': 1.1}




def set_up_delta_env()


    all_teams = pd.read_csv('all_teams.csv')
    all_competitions = pd.read_csv('all_competitions.csv')

    ### Get all the fixtures, teams and competitions
    all_events = get_all_events()
    
    # Check that all the competitions and teams exist
    proceed = check_all_teams_exist(all_events, all_teams)
    proceed = check_competitions_exist(all_events, all_competitions)
    
    float_columns = ['home_pre_delta', 'home_post_delta', 'away_pre_delta', 'away_post_delta', 'pre_delta_diff', 'home_team_buffer', 'home_pre_delta_halftime', 'home_post_delta_halftime', 'away_pre_delta_halftime', 'away_post_delta_halftime', 'pre_delta_diff_halftime', 'home_team_buffer_halftime', 'home_pre_delta_secondhalf', 'home_post_delta_secondhalf', 'away_pre_delta_secondhalf', 'away_post_delta_secondhalf', 'pre_delta_diff_secondhalf', 'home_team_buffer_secondhalf', 'pre_delta_diff_adjusted', 'pre_delta_adjustment', 'pre_delta_adjustment_halftime', 'pre_delta_diff_halftime_adjusted', 'pre_delta_adjustment_secondhalf', 'pre_delta_diff_secondhalf_adjusted']
    all_previous_deltas = get_all_previous_deltas(float_columns)


    # Remove any faulty events
    all_events = remove_faulty_fixtures(all_events)
    all_events['total_points'] = all_events[['home_score', 'away_score']].apply(lambda x: x[0] + x[1] if ( pd.notna(x[0]) & pd.notna(x[1]) ) * ((x[0] > 0) | (x[1] > 0) ) else None, axis = 1)

    # Remove any events that no longer exist for whatever reason
    check_for_nonexistant_events(all_previous_deltas, all_events)

    # Check for duplicate events
    duplicate_events = check_for_duplicate_events(all_events)
    
    if len(duplicate_events):
        
        earliest_duplicate_event = all_events[ all_events['event_id'].isin(duplicate_events)].index.min()
        all_events = all_events.loc[ :earliest_duplicate_event]

        error_string = 'There are duplicate events that need fixed before the event deltas can be calculated - ' + str(duplicate_events)[1:-1                                                                                                                ]
        notifyTeams(error_string)
        print(error_string)
        proceed = False
    

    # Check to see if current events competition id and scores match up with what was calculated before
    previous_deltas_to_keep = check_fixtures_that_have_changed(all_previous_deltas, all_events)


    # Check to see if any new events have been added historically
    all_events = check_for_any_new_events(all_events, previous_deltas_to_keep, float_columns)


    all_competitions['level'] = all_competitions['level'].apply(lambda x: float(x) if x != 'na' else None)
    for key in new_comp_levels:
        all_competitions['level'] = all_competitions[['id', 'level']].apply(lambda x: new_comp_levels[x[0]] if x[0] == key else x[1] , axis = 1)



    ### Add in fixture numbers
    all_events = get_team_fixture_numbers(all_events)
    all_events = all_events.sort_values(['start_time', 'home_team_total_fixture_number', 'away_team_total_fixture_number'])


    ## Add venue info
    all_events, venues = add_venue_info(all_events)
    ## Add home venue info
    all_events = set_home_venues(all_events, all_teams, venues)


    ### Add competition details
    all_events = all_events.merge(all_competitions[['id', 'name', 'home_competition_group', 'level']].rename(columns = {'name':'competition_name'}), how = 'left', left_on = 'competition_internal_id', right_on = 'id')


    ### Add competition fixture numbers
    all_events = get_competition_fixture_numbers()


    ### Make sure the sort order is still chronological
    all_events = all_events.sort_values(['start_time', 'home_team_total_fixture_number', 'away_team_total_fixture_number'])

    # Set any ambigious halftime scores to None
    halftime_zero_indexes = all_events[ (all_events['home_halftime_score'] == 0) & (all_events['away_halftime_score'] == 0) ].index
    all_events.loc[halftime_zero_indexes, 'home_halftime_score'] = None
    all_events.loc[halftime_zero_indexes, 'away_halftime_score'] = None
    del halftime_zero_indexes

    # Make sure future events are set to None and not 0 for their scores
    future_events = all_events[ all_events['start_time'] >= str(datetime.datetime.now())].index
    all_events.loc[future_events, 'home_halftime_score'] = None
    all_events.loc[future_events, 'away_halftime_score'] = None



    ### Convert team dicts to dataframes
    international_rankings.update(international_rankings_women)
    internationl_rankings_df = team_dict_to_dataframe(international_rankings)

    ### Set start range of calculations
    start_range = all_events[ pd.isna(all_events['home_pre_delta']) | pd.isna(all_events['home_post_delta']) | pd.isna(all_events['away_pre_delta']) | pd.isna(all_events['away_post_delta'])].index[0]
    print(start_range)
    start_range = max(all_events[ all_events['start_time'] >= '2010-01-01'].index.min(), start_range)
    #start_range = min(all_events[ all_events['start_time'] >= '2010-01-01'].index.min(), start_range)
    
    end_range = all_events.index.max()
    print('Calculation Deltas from', all_events.loc[start_range, 'start_time'], all_events.loc[start_range, 'name'], all_events.loc[start_range, 'event_id'])

    #start_range = all_events[ all_events['start_time'] >= '2010-01-01'].index.min()
    
    max_points_win = 5
    win_margin_buffer = 0
    level_setting = 40
    win_bonus = 0

    home_team_fixture_column = 'home_team_total_fixture_number'
    away_team_fixture_column = 'away_team_total_fixture_number'


    return

 

def add_win_margin():

    ################################## Win Margin ##################################
            
    delta_column_to_calcuate = 'win_margin'
    all_events[delta_column_to_calcuate] = (all_events['home_score'] - all_events['away_score'])
    
    ### Get standard home win margins to use in the algorithm
    all_base_home_win_margin, international_mens_base_home_win_margin, international_womens_base_home_win_margin, competition_win_margin_means = get_comp_standards(all_events, delta_column_to_calcuate)

    ### Set column names
    home_pre_delta_name = 'home_pre_delta'
    home_post_delta_name = 'home_post_delta'
    away_pre_delta_name = 'away_pre_delta'
    away_post_delta_name = 'away_post_delta'
    pre_delta_diff_name = 'pre_delta_diff'
    home_team_buffer_name = 'home_team_buffer'
    post_delta_adjustment_name = 'pre_delta_adjustment'
    pre_delta_diff_adjusted = 'pre_delta_diff_adjusted'
    home_error = pre_delta_diff_name + '_home_team_home_error'
    away_error = pre_delta_diff_name + '_away_team_away_error'
    
    if home_error not in all_events.columns:
        all_events[home_error] = None
    if away_error not in all_events.columns:
        all_events[away_error] = None

    all_events = generate_elo_ranks(all_events, delta_column_to_calcuate, post_delta_adjustment_name, home_pre_delta_name, home_post_delta_name, away_pre_delta_name, away_post_delta_name, pre_delta_diff_name, home_team_buffer_name, max_points_win, win_margin_buffer, level_setting, start_range, end_range, list_order_int_men, list_order_int_women_age, list_order_club, win_bonus, all_competitions, all_teams, home_team_fixture_column, away_team_fixture_column, home_error, away_error)
    
        

    # Remove events that haven't been calculated - Potentially combined events affecting fixture numbers being calculated properly?
    events_to_remove = list(all_events[ pd.isna(all_events['pre_delta_diff']) ]['event_id'])
    if len(events_to_remove) > 0:
        all_events = all_events[ ~all_events['event_id'].isin(events_to_remove)]
        message = 'There are events where the pre_delta_diff could not be calculated, please check.  Select * from materialised_view_event where event_id in (' + str(events_to_remove) + ');'
        notifyTeams(message)
        ### Add in fixture numbers
        all_events = get_team_fixture_numbers(all_events)
        all_events = all_events.sort_values(['start_time', 'home_team_total_fixture_number', 'away_team_total_fixture_number'])
        all_events.reset_index(drop = True, inplace = True)
        
        ### Set start range of calculations
        #start_range = all_events[ pd.isna(all_events['home_pre_delta']) | pd.isna(all_events['home_post_delta']) | pd.isna(all_events['away_pre_delta']) | pd.isna(all_events['away_post_delta'])].index[0]
        start_range = max(all_events[ all_events['start_time'] >= '2010-01-01'].index.min(), start_range)
        end_range = all_events.index.max()
        print('Calculation Deltas from', all_events.loc[start_range, 'start_time'])
        
        
    sql_statement = "select event_id from event_deltas;"
    current_event_ids, error = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = True)

    
    current_events = all_events.loc[start_range:]
    current_events = current_events[ (current_events['event_id'].isin(list(current_event_ids['event_id']))) ]
    new_events = all_events.loc[start_range:]
    new_events = new_events[ ~new_events['event_id'].isin(list(current_event_ids['event_id'])) ]
    
    new_events_venue = new_events[ pd.notna(new_events['venue_internal_id'])]
    if len(new_events_venue)>0:
        formatted_data = new_events_venue[[
    'event_id',
    'home_team_internal_id',
    'away_team_internal_id',
    'competition_internal_id',
    'venue_internal_id',
    'start_time',
    'home_score',
    'away_score',
    'home_pre_delta',
    'away_pre_delta',
    'pre_delta_diff',
    'pre_delta_diff_adjusted',
    'home_post_delta',
    'away_post_delta',
    'home_team_buffer',
    'pre_delta_adjustment', home_error, away_error]]
        powerbi_table_info, error = get_table_info('event_deltas')
        formatted_data = format_data_for_postgres(formatted_data, powerbi_table_info)
        insert_sql('event_deltas', formatted_data)
        
        
    new_events_novenue = new_events[ pd.isna(new_events['venue_internal_id'])]
    if len(new_events_novenue)>0:
        formatted_data = new_events_novenue[[
    'event_id',
    'home_team_internal_id',
    'away_team_internal_id',
    'competition_internal_id',
    'venue_internal_id',
    'start_time',
    'home_score',
    'away_score',
    'home_pre_delta',
    'away_pre_delta',
    'pre_delta_diff',
    'pre_delta_diff_adjusted',
    'home_post_delta',
    'away_post_delta',
    'home_team_buffer',
    'pre_delta_adjustment', home_error, away_error]]
        powerbi_table_info, error = get_table_info('event_deltas')
        formatted_data = format_data_for_postgres(formatted_data, powerbi_table_info)
        insert_sql('event_deltas', formatted_data)
    
    
    current_events_venue = current_events[ pd.notna(current_events['venue_internal_id']) & (current_events['venue_internal_id'] != 'None')]
    if len(current_events_venue)>0:
        formatted_data = current_events_venue[[
    'event_id',
    'home_team_internal_id',
    'away_team_internal_id',
    'competition_internal_id',
    'venue_internal_id',
    'start_time',
    'home_score',
    'away_score',
    'home_pre_delta',
    'away_pre_delta',
    'pre_delta_diff',
    'pre_delta_diff_adjusted',
    'home_post_delta',
    'away_post_delta',
    'home_team_buffer',
    'pre_delta_adjustment', home_error, away_error]]
        powerbi_table_info, error = get_table_info('event_deltas')
        formatted_data = format_data_for_postgres(formatted_data, powerbi_table_info)
        update_sql('event_deltas', formatted_data, 'event_id')
        
    
    current_events_novenue = current_events[ pd.isna(current_events['venue_internal_id'])]
    if len(current_events_novenue)>0:
        formatted_data = current_events_novenue[[
    'event_id',
    'home_team_internal_id',
    'away_team_internal_id',
    'competition_internal_id',
    'start_time',
    'home_score',
    'away_score',
    'home_pre_delta',
    'away_pre_delta',
    'pre_delta_diff',
    'pre_delta_diff_adjusted',
    'home_post_delta',
    'away_post_delta',
    'home_team_buffer',
    'pre_delta_adjustment', home_error, away_error]]
        powerbi_table_info, error = get_table_info('event_deltas')
        formatted_data = format_data_for_postgres(formatted_data, powerbi_table_info)
        update_sql('event_deltas', formatted_data, 'event_id')
        
        
    ########################################################################
    ########################################################################
    ########################################################################

    return



def add_first_half():
    
        
    ########################### First Half ###########################
    delta_column_to_calcuate = 'half_time_win_margin'
    all_events[delta_column_to_calcuate] = all_events[['home_halftime_score', 'away_halftime_score']].apply(lambda x: x[0] - x[1] if (pd.notna(x[0]) and pd.notna(x[1])) and ((x[0]>0) | (x[1]>0)) else None )


    ### Get standard home win margins to use in the algorithm
    all_base_home_win_margin, international_mens_base_home_win_margin, international_womens_base_home_win_margin, competition_win_margin_means = get_comp_standards(all_events, delta_column_to_calcuate)

    home_pre_delta_name = 'home_pre_delta_halftime'
    home_post_delta_name = 'home_post_delta_halftime'
    away_pre_delta_name = 'away_pre_delta_halftime'
    away_post_delta_name = 'away_post_delta_halftime'
    pre_delta_diff_name = 'pre_delta_diff_halftime'
    home_team_buffer_name = 'home_team_buffer_halftime'
    post_delta_adjustment_name = 'pre_delta_adjustment_halftime'
    pre_delta_diff_adjusted = 'pre_delta_diff_halftime_adjusted'
    home_error = pre_delta_diff_name + '_home_team_home_error'
    away_error = pre_delta_diff_name + '_away_team_away_error'

    if home_error not in all_events.columns:
        all_events[home_error] = None
    if away_error not in all_events.columns:
        all_events[away_error] = None
        

    all_events = generate_elo_ranks(all_events, delta_column_to_calcuate, post_delta_adjustment_name, home_pre_delta_name, home_post_delta_name, away_pre_delta_name, away_post_delta_name, pre_delta_diff_name, home_team_buffer_name, max_points_win, win_margin_buffer, level_setting, start_range, end_range, list_order_int_men, list_order_int_women_age, list_order_club, win_bonus, all_competitions, all_teams, home_team_fixture_column, away_team_fixture_column, home_error, away_error)

    events_to_remove = list(all_events[ pd.isna(all_events['pre_delta_diff_halftime']) ]['event_id'])
    if len(events_to_remove) > 0:
        temp_events = all_events[ ~all_events['event_id'].isin(events_to_remove)]
        message = 'There are events where the pre_delta_diff could not be calculated, please check.  Select * from materialised_view_event where event_id in (' + str(events_to_remove) + ');'
        notifyTeams(message)
    else:
        temp_events = all_events
        
    formatted_data = all_events.loc[start_range:]
    formatted_data = formatted_data[[
    'event_id',
    'home_pre_delta_halftime',
    'home_post_delta_halftime',
    'away_pre_delta_halftime',
    'away_post_delta_halftime',
    'pre_delta_diff_halftime',
    'pre_delta_diff_halftime_adjusted',
    'home_team_buffer_halftime',
    'pre_delta_adjustment_halftime', home_error, away_error]]
    powerbi_table_info, error = get_table_info('event_deltas')
    formatted_data = format_data_for_postgres(formatted_data, powerbi_table_info)
    update_sql('event_deltas', formatted_data, 'event_id')
    ########################################################################
    ########################################################################
    ########################################################################



def add_second_half():
    
    
    ########################### Second Half ###########################
    delta_column_to_calcuate = 'second_half_win_margin'

    all_events[delta_column_to_calcuate] = (all_events['home_score'] - all_events['away_score']) - (all_events['home_halftime_score'] - all_events['away_halftime_score'])

    all_base_home_win_margin, international_mens_base_home_win_margin, international_womens_base_home_win_margin, competition_win_margin_means = get_comp_standards(all_events, delta_column_to_calcuate)

    home_pre_delta_name = 'home_pre_delta_secondhalf'
    home_post_delta_name = 'home_post_delta_secondhalf'
    away_pre_delta_name = 'away_pre_delta_secondhalf'
    away_post_delta_name = 'away_post_delta_secondhalf'
    pre_delta_diff_name = 'pre_delta_diff_secondhalf'
    home_team_buffer_name = 'home_team_buffer_secondhalf'
    post_delta_adjustment_name = 'pre_delta_adjustment_secondhalf'
    pre_delta_diff_adjusted = 'pre_delta_diff_secondhalf_adjusted'
    home_error = pre_delta_diff_name + '_home_team_home_error'
    away_error = pre_delta_diff_name + '_away_team_away_error'

    if home_error not in all_events.columns:
        all_events[home_error] = None
    if away_error not in all_events.columns:
        all_events[away_error] = None
        
    all_events = generate_elo_ranks(all_events, delta_column_to_calcuate, post_delta_adjustment_name, home_pre_delta_name, home_post_delta_name, away_pre_delta_name, away_post_delta_name, pre_delta_diff_name, home_team_buffer_name, max_points_win, win_margin_buffer, level_setting, start_range, end_range, list_order_int_men, list_order_int_women_age, list_order_club, win_bonus, all_competitions, all_teams, home_team_fixture_column, away_team_fixture_column, home_error, away_error)

    events_to_remove = list(all_events[ pd.isna(all_events['pre_delta_diff_secondhalf']) ]['event_id'])
    if len(events_to_remove) > 0:
        temp_events = all_events[ ~all_events['event_id'].isin(events_to_remove)]
        message = 'There are events where the pre_delta_diff could not be calculated, please check.  Select * from materialised_view_event where event_id in (' + str(events_to_remove) + ');'
        notifyTeams(message)
    else:
        temp_events = all_events
        
    formatted_data = all_events.loc[start_range:]
    formatted_data = formatted_data[[
    'event_id',
    'home_pre_delta_secondhalf',
    'home_post_delta_secondhalf',
    'away_pre_delta_secondhalf',
    'away_post_delta_secondhalf',
    'pre_delta_diff_secondhalf',
    'pre_delta_diff_secondhalf_adjusted',
    'home_team_buffer_secondhalf',
    'pre_delta_adjustment_secondhalf', home_error, away_error]]
    powerbi_table_info, error = get_table_info('event_deltas')
    formatted_data = format_data_for_postgres(formatted_data, powerbi_table_info)
    update_sql('event_deltas', formatted_data, 'event_id')
    ########################################################################
    ########################################################################
    ########################################################################



def add_total_points():
    
    all_previous_deltas = get_all_previous_deltas(float_columns)

    tp_columns = [
        'event_id',
        'home_attack_pre',
        'home_defence_pre',
        'home_home_attack_pre',
        'home_home_defence_pre',
        'away_attack_pre',
        'away_defence_pre',
        'away_away_attack_pre',
        'away_away_defence_pre',
        'home_attack_post',
        'home_defence_post',
        'home_home_attack_post',
        'home_home_defence_post',
        'away_attack_post',
        'away_defence_post',
        'away_away_attack_post',
        'away_away_defence_post',
        'pred_home_score_all',
        'pred_home_score_ha', 
        'pred_away_score_all', 
        'pred_away_score_ha',
        'pred_total_points_all',
        'pred_total_points_ha',
        'pred_total_points_all_adjusted',
        'pred_total_points_ha_adjusted']



    for col in all_events.columns:
        if (col in tp_columns) & (col != 'event_id') :
            all_events.drop(col, axis = 1, inplace = True)


    all_events = all_events.merge(all_previous_deltas[tp_columns])



    ##################################################################################################
    ########################################## Total Points ##########################################
    ##################################################################################################

    home_pre_delta_name_1 = 'home_attack_pre'
    home_pre_delta_name_2 = 'home_defence_pre'
    home_pre_delta_name_3 = 'home_home_attack_pre'
    home_pre_delta_name_4 = 'home_home_defence_pre'

    away_pre_delta_name_1 = 'away_attack_pre'
    away_pre_delta_name_2 = 'away_defence_pre'
    away_pre_delta_name_3 = 'away_away_attack_pre'
    away_pre_delta_name_4 = 'away_away_defence_pre'

    home_post_delta_name_1 = 'home_attack_post'
    home_post_delta_name_2 = 'home_defence_post'
    home_post_delta_name_3 = 'home_home_attack_post'
    home_post_delta_name_4 = 'home_home_defence_post'

    away_post_delta_name_1 = 'away_attack_post'
    away_post_delta_name_2 = 'away_defence_post'
    away_post_delta_name_3 = 'away_away_attack_post'
    away_post_delta_name_4 = 'away_away_defence_post'

    delta_column_to_calcuate_1 = 'home_score'
    delta_column_to_calcuate_2 = 'away_score'

    get_home_advantage = True
    all_events = calculate_total_points_deltas()
    #update_event_deltas_total_points(all_events, start_range, end_range, home_pre_delta_name_1, home_pre_delta_name_2, home_pre_delta_name_3, home_pre_delta_name_4, away_pre_delta_name_1, away_pre_delta_name_2, away_pre_delta_name_3, away_pre_delta_name_4, home_post_delta_name_1, home_post_delta_name_2, home_post_delta_name_3, home_post_delta_name_4, away_post_delta_name_1, away_post_delta_name_2, away_post_delta_name_3, away_post_delta_name_4)


    all_events['pred_total_points_all'] = all_events[['pred_home_score_all', 'pred_away_score_all']].apply(lambda x: max(0,x[0]) + max(0,x[1]), axis = 1 )
    all_events['pred_total_points_ha'] = all_events[['pred_home_score_ha', 'pred_away_score_ha']].apply(lambda x: max(0,x[0]) + max(0,x[1]), axis = 1 )
    all_events['pred_total_points_all_adjusted'] = all_events[['pred_total_points_all', 'pre_delta_diff', 'pre_delta_adjustment']].apply(lambda x: x[0] - ( -0.00175*((x[1] - x[2])*(x[1] - x[2]))), axis = 1 )
    all_events['pred_total_points_ha_adjusted'] = all_events[['pred_total_points_ha', 'pre_delta_diff', 'pre_delta_adjustment']].apply(lambda x: x[0] - ( -0.00175*((x[1] - x[2])*(x[1] - x[2]))), axis = 1 )

    formatted_data = all_events.loc[start_range:]
    formatted_data = formatted_data[[
    'event_id',
    'home_attack_pre',
    'home_defence_pre',
    'home_home_attack_pre',
    'home_home_defence_pre',
    'away_attack_pre',
    'away_defence_pre',
    'away_away_attack_pre',
    'away_away_defence_pre',
    'home_attack_post',
    'home_defence_post',
    'home_home_attack_post',
    'home_home_defence_post',
    'away_attack_post',
    'away_defence_post',
    'away_away_attack_post',
    'away_away_defence_post',
    'pred_home_score_all',
    'pred_home_score_ha', 
    'pred_away_score_all', 
    'pred_away_score_ha',
    'pred_total_points_all',
    'pred_total_points_ha',
    'pred_total_points_all_adjusted',
    'pred_total_points_ha_adjusted']]

    powerbi_table_info, error = get_table_info('event_deltas')
    formatted_data = format_data_for_postgres(formatted_data, powerbi_table_info)
    update_sql('event_deltas', formatted_data, 'event_id')
    ##############################################################################################################################
    ##############################################################################################################################
    ##############################################################################################################################



def add_trends():
    
        
    ############################################################################################
    ########################################## Trends ##########################################
    ############################################################################################

    # Get all event deltas
    sql_statement = "select * from event_deltas;"
    event_deltas, error_occured_new = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, True)


    # Add all event deltas to all events
    all_events = add_event_deltas(all_events, event_deltas, float_columns)


    ########################### Win Margin ###########################
    delta_column_to_calcuate = 'win_margin'
    all_events[delta_column_to_calcuate] = (all_events['home_score'] - all_events['away_score'])

    ### Set column names
    home_pre_delta_name = 'home_pre_delta'
    home_post_delta_name = 'home_post_delta'
    away_pre_delta_name = 'away_pre_delta'
    away_post_delta_name = 'away_post_delta'
    pre_delta_diff_name = 'pre_delta_diff'
    home_team_buffer_name = 'home_team_buffer'

    delta_change_name = 'delta_change'
    error_name = 'error'

    # Delta change trend
    all_events['home_delta_change'] = all_events[home_post_delta_name] - all_events[home_pre_delta_name]
    all_events['away_delta_change'] = all_events[away_post_delta_name] - all_events[away_pre_delta_name]
    # Error trend
    all_events['home_error'] = all_events[pre_delta_diff_name] - all_events[delta_column_to_calcuate]
    all_events['away_error'] = -all_events[pre_delta_diff_name] - -all_events[delta_column_to_calcuate]

    # Uses start range calculated earlier
    for num_games in [5, 10, 20]:

        trend_name = delta_change_name
        trend_type = 'delta_change'
        all_events = add_trends(all_events, trend_name, trend_type, num_games, start_range)

        trend_name = error_name
        trend_type = 'delta_change'
        all_events = add_trends(all_events, trend_name, trend_type, num_games, start_range)

    update_event_deltas_trends(all_events, delta_change_name, error_name)
    #################################################################################


    ########################### First Half ###########################
    delta_column_to_calcuate = 'half_time_win_margin'
    all_events[delta_column_to_calcuate] = (all_events['home_halftime_score'] - all_events['away_halftime_score'])

    home_pre_delta_name = 'home_pre_delta_halftime'
    home_post_delta_name = 'home_post_delta_halftime'
    away_pre_delta_name = 'away_pre_delta_halftime'
    away_post_delta_name = 'away_post_delta_halftime'
    pre_delta_diff_name = 'pre_delta_diff_halftime'
    home_team_buffer_name = 'home_team_buffer_halftime'

    delta_change_name = 'delta_change_halftime'
    error_name = 'error_halftime'


    # Delta change trend
    all_events['home_delta_change'] = all_events[home_post_delta_name] - all_events[home_pre_delta_name]
    all_events['away_delta_change'] = all_events[away_post_delta_name] - all_events[away_pre_delta_name]
    # Error trend
    all_events['home_error'] = all_events[pre_delta_diff_name] - all_events[delta_column_to_calcuate]
    all_events['away_error'] = -all_events[pre_delta_diff_name] - -all_events[delta_column_to_calcuate]

    # Uses start range calculated earlier
    for num_games in [5, 10, 20]:

        trend_name = delta_change_name
        trend_type = 'delta_change'
        all_events = add_trends(all_events, trend_name, trend_type, num_games, start_range)

        trend_name = error_name
        trend_type = 'delta_change'
        all_events = add_trends(all_events, trend_name, trend_type, num_games, start_range)

    update_event_deltas_trends(all_events, delta_change_name, error_name)
    #################################################################################



    ########################### Second Half ###########################
    delta_column_to_calcuate = 'second_half_win_margin'

    all_events[delta_column_to_calcuate] = (all_events['home_score'] - all_events['away_score']) - (all_events['home_halftime_score'] - all_events['away_halftime_score'])

    home_pre_delta_name = 'home_pre_delta_secondhalf'
    home_post_delta_name = 'home_post_delta_secondhalf'
    away_pre_delta_name = 'away_pre_delta_secondhalf'
    away_post_delta_name = 'away_post_delta_secondhalf'
    pre_delta_diff_name = 'pre_delta_diff_secondhalf'
    home_team_buffer_name = 'home_team_buffer_secondhalf'

    delta_change_name = 'delta_change_secondhalf'
    error_name = 'error_secondhalf'


    # Delta change trend
    all_events['home_delta_change'] = all_events[home_post_delta_name] - all_events[home_pre_delta_name]
    all_events['away_delta_change'] = all_events[away_post_delta_name] - all_events[away_pre_delta_name]
    # Error trend
    all_events['home_error'] = all_events[pre_delta_diff_name] - all_events[delta_column_to_calcuate]
    all_events['away_error'] = -all_events[pre_delta_diff_name] - -all_events[delta_column_to_calcuate]

    # Uses start range calculated earlier
    for num_games in [5, 10, 20]:

        trend_name = delta_change_name
        trend_type = 'delta_change'
        all_events = add_trends(all_events, trend_name, trend_type, num_games, start_range)

        trend_name = error_name
        trend_type = 'delta_change'
        all_events = add_trends(all_events, trend_name, trend_type, num_games, start_range)

    update_event_deltas_trends(all_events, delta_change_name, error_name)
    #################################################################################



def main(req: func.HttpRequest) -> func.HttpResponse:
    print('Yes')
    logging.info('Python HTTP trigger function processed a request.')

    # Extract the "message" parameter from the request URL
    message = req.params.get('message')
    if not message:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            message = req_body.get('message')

    message = str(message) + '_' + str(db_pass)
    if message:
        
        set_up_delta_env()
        
        add_win_margin()
        
        add_first_half()
        
        add_second_half()
        
        add_total_points()
        
        add_trends()
        
        
        notifyTeams(message)
        return func.HttpResponse(f"Message sent: {message}")
    else:
        return func.HttpResponse(
             "Please pass a message on the query string or in the request body",
             status_code=400
        )
