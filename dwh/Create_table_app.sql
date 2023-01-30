-- CreateEnum
CREATE TYPE "Gender" AS ENUM ('Male', 'Female');
-- CreateEnum
CREATE TYPE "District" AS ENUM (
    'Islands',
    'Kwai_tsing',
    'North',
    'Sai_kung',
    'Sha_tin',
    'Tai_po',
    'Tsuen_wan',
    'Tuen_mun',
    'Yuen_long',
    'Kowloon_city',
    'Kwun_tong',
    'Sham_shui_po',
    'Wong_tai_sin',
    'Yau_tsim_mong',
    'Central_and_western',
    'Eastern',
    'Southern',
    'Wan_chai'
);
-- CreateEnum
CREATE TYPE "Weekday" AS ENUM (
    'Monday',
    'Tuesday',
    'Wednesday',
    'Thursday',
    'Friday',
    'Saturday',
    'Sunday'
);
-- CreateEnum
CREATE TYPE "AnimalType" AS ENUM ('Cat', 'Dog', 'Others');
-- CreateEnum
CREATE TYPE "ThumbStat" AS ENUM ('Up', 'Down', 'Removed');
-- CreateTable
CREATE TABLE "users" (
    "id" SERIAL NOT NULL,
    "email" TEXT NOT NULL,
    "username" TEXT NOT NULL,
    "password" TEXT NOT NULL,
    "phone_number" INTEGER NOT NULL,
    "gender" "Gender" NOT NULL,
    "district" "District" NOT NULL,
    "year_birth" INTEGER,
    "month_birth" INTEGER,
    "is_writer" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "users_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "map_type" (
    "id" SERIAL NOT NULL,
    "chinese_type" VARCHAR(255) NOT NULL,
    "english_type" VARCHAR(255) NOT NULL,
    CONSTRAINT "map_type_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "maps" (
    "id" SERIAL NOT NULL,
    "map_type_id" INTEGER NOT NULL,
    "chinese_name" TEXT NOT NULL,
    "english_name" TEXT NOT NULL,
    "email" TEXT,
    "phone_number" INTEGER,
    "chinese_address" TEXT NOT NULL,
    "english_address" TEXT NOT NULL,
    "district" "District" NOT NULL,
    "latitude" DECIMAL(11, 8) NOT NULL,
    "longitude" DECIMAL(11, 8) NOT NULL,
    "profile_img" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "maps_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "working_hours" (
    "id" SERIAL NOT NULL,
    "map_id" INTEGER NOT NULL,
    "weekday" "Weekday" NOT NULL,
    "hours" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "working_hours_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "pet_img_tags" (
    "id" SERIAL NOT NULL,
    "name" TEXT NOT NULL,
    "tag_count" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "pet_img_tags_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "exp_tags" (
    "id" SERIAL NOT NULL,
    "name" TEXT NOT NULL,
    "tag_count" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "exp_tags_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "event_tags" (
    "id" SERIAL NOT NULL,
    "name" TEXT NOT NULL,
    "tag_count" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "event_tags_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "species" (
    "id" SERIAL NOT NULL,
    "family_id" INTEGER,
    "chinese_species" TEXT NOT NULL,
    "english_species" TEXT NOT NULL,
    CONSTRAINT "species_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "pets" (
    "id" SERIAL NOT NULL,
    "user_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "species_id" INTEGER NOT NULL,
    "gender" "Gender",
    "profile_img" TEXT,
    "date_birth" DATE,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "pets_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "pets_weight" (
    "id" SERIAL NOT NULL,
    "pet_id" INTEGER NOT NULL,
    "weight" DECIMAL(6, 2) NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "pets_weight_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "pets_img" (
    "id" SERIAL NOT NULL,
    "pet_id" INTEGER NOT NULL,
    "tag" TEXT,
    "name" TEXT NOT NULL,
    "is_private" BOOLEAN NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "pets_img_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "pets_vaccine" (
    "id" SERIAL NOT NULL,
    "pet_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "pets_vaccine_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "comments" (
    "id" SERIAL NOT NULL,
    "user_id" INTEGER NOT NULL,
    "map_id" INTEGER NOT NULL,
    "title" VARCHAR(255) NOT NULL,
    "content" TEXT NOT NULL,
    "is_thumb" BOOLEAN NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "comments_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "comments_img" (
    "id" SERIAL NOT NULL,
    "comment_id" INTEGER NOT NULL,
    "name" VARCHAR(255) NOT NULL,
    CONSTRAINT "comments_img_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "events" (
    "id" SERIAL NOT NULL,
    "host_id" INTEGER NOT NULL,
    "location" VARCHAR(255) NOT NULL,
    "tag" TEXT,
    "animal_type" "AnimalType" NOT NULL,
    "title" VARCHAR(255) NOT NULL,
    "content" TEXT NOT NULL,
    "date" DATE NOT NULL,
    "time" TIME(6) NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "events_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "experience" (
    "id" SERIAL NOT NULL,
    "poster_id" INTEGER NOT NULL,
    "title" VARCHAR(255) NOT NULL,
    "tag" TEXT,
    "content" TEXT NOT NULL,
    "map_id" INTEGER NOT NULL,
    "num_thumbs_up" INTEGER NOT NULL DEFAULT 0,
    "num_thumbs_down" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "experience_pkey" PRIMARY KEY ("id")
);
-- CreateTable
CREATE TABLE "experience_like" (
    "id" SERIAL NOT NULL,
    "user_id" INTEGER NOT NULL,
    "exp_id" INTEGER NOT NULL,
    "thumb_status" "ThumbStat" NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "experience_like_pkey" PRIMARY KEY ("id")
);
-- CreateIndex
CREATE UNIQUE INDEX "users_email_key" ON "users"("email");
-- CreateIndex
CREATE UNIQUE INDEX "users_username_key" ON "users"("username");
-- CreateIndex
CREATE UNIQUE INDEX "users_phone_number_key" ON "users"("phone_number");
-- CreateIndex
CREATE UNIQUE INDEX "map_type_chinese_type_english_type_key" ON "map_type"("chinese_type", "english_type");
-- CreateIndex
CREATE UNIQUE INDEX "maps_chinese_address_key" ON "maps"("chinese_address");
-- CreateIndex
CREATE UNIQUE INDEX "maps_english_address_key" ON "maps"("english_address");
-- CreateIndex
CREATE UNIQUE INDEX "maps_profile_img_key" ON "maps"("profile_img");
-- CreateIndex
CREATE UNIQUE INDEX "maps_latitude_longitude_key" ON "maps"("latitude", "longitude");
-- CreateIndex
CREATE UNIQUE INDEX "pet_img_tags_name_key" ON "pet_img_tags"("name");
-- CreateIndex
CREATE UNIQUE INDEX "exp_tags_name_key" ON "exp_tags"("name");
-- CreateIndex
CREATE UNIQUE INDEX "event_tags_name_key" ON "event_tags"("name");
-- CreateIndex
CREATE UNIQUE INDEX "species_chinese_species_english_species_key" ON "species"("chinese_species", "english_species");
-- CreateIndex
CREATE UNIQUE INDEX "pets_profile_img_key" ON "pets"("profile_img");
-- CreateIndex
CREATE UNIQUE INDEX "pets_user_id_name_key" ON "pets"("user_id", "name");
-- CreateIndex
CREATE UNIQUE INDEX "pets_img_name_key" ON "pets_img"("name");
-- CreateIndex
CREATE UNIQUE INDEX "comments_user_id_map_id_key" ON "comments"("user_id", "map_id");
-- CreateIndex
CREATE UNIQUE INDEX "events_host_id_date_time_key" ON "events"("host_id", "date", "time");
-- CreateIndex
CREATE UNIQUE INDEX "experience_like_user_id_exp_id_key" ON "experience_like"("user_id", "exp_id");
-- AddForeignKey
ALTER TABLE "maps"
ADD CONSTRAINT "maps_map_type_id_fkey" FOREIGN KEY ("map_type_id") REFERENCES "map_type"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "working_hours"
ADD CONSTRAINT "working_hours_map_id_fkey" FOREIGN KEY ("map_id") REFERENCES "maps"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "species"
ADD CONSTRAINT "species_family_id_fkey" FOREIGN KEY ("family_id") REFERENCES "species"("id") ON DELETE
SET NULL ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "pets"
ADD CONSTRAINT "pets_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "pets"
ADD CONSTRAINT "pets_species_id_fkey" FOREIGN KEY ("species_id") REFERENCES "species"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "pets_weight"
ADD CONSTRAINT "pets_weight_pet_id_fkey" FOREIGN KEY ("pet_id") REFERENCES "pets"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "pets_img"
ADD CONSTRAINT "pets_img_pet_id_fkey" FOREIGN KEY ("pet_id") REFERENCES "pets"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "pets_vaccine"
ADD CONSTRAINT "pets_vaccine_pet_id_fkey" FOREIGN KEY ("pet_id") REFERENCES "pets"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "comments"
ADD CONSTRAINT "comments_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "comments"
ADD CONSTRAINT "comments_map_id_fkey" FOREIGN KEY ("map_id") REFERENCES "maps"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "comments_img"
ADD CONSTRAINT "comments_img_comment_id_fkey" FOREIGN KEY ("comment_id") REFERENCES "comments"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "events"
ADD CONSTRAINT "events_host_id_fkey" FOREIGN KEY ("host_id") REFERENCES "users"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "experience"
ADD CONSTRAINT "experience_poster_id_fkey" FOREIGN KEY ("poster_id") REFERENCES "users"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "experience"
ADD CONSTRAINT "experience_map_id_fkey" FOREIGN KEY ("map_id") REFERENCES "maps"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "experience_like"
ADD CONSTRAINT "experience_like_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
-- AddForeignKey
ALTER TABLE "experience_like"
ADD CONSTRAINT "experience_like_exp_id_fkey" FOREIGN KEY ("exp_id") REFERENCES "experience"("id") ON DELETE RESTRICT ON UPDATE CASCADE;